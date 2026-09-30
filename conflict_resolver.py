#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多来源记录键冲突状态机（纯标准库，单文件）

====================== 输入格式（自定，理由附后） ======================
行式指令流，每行一条指令，`#` 开头或空行忽略：

  REC <键> <来源> <优先级> <值...>     流入一条记录（值可含空格；值中 @键 表示引用）
  RESOLVE <键>                         对处于待消解状态的键发起规则竞争裁决
  UNDO <键>                            撤销已裁决键的消解，回到待消解状态
  LIMIT <n>                            设置同键冲突累积上限（默认 4）
  TRUST <来源>                         将来源标记为可信（供“可信来源”规则作上下文）

为什么用这个格式：
  1. 行式、命令前缀明确 —— 模拟“记录流逐条流入”，解析只需 str.split，零依赖；
  2. 值放最后可含空格，@键 引用内联在值里，无需额外引用记录类型；
  3. RESOLVE/UNDO 显式触发，能完整展示 待消解→已裁决→撤销 的状态迁移过程。

====================== 状态机 ======================
  CLEAN     键只有 0/1 条记录，无冲突
  PENDING   同键出现多条不同记录，冲突待消解（可持续累积，超限报错）
  RESOLVED  规则竞争裁决完成，键值确定，引用该键的记录同步更新
  RESOLVED --UNDO--> PENDING   撤销消解，回到待消解并报告
  RESOLVED --新REC--> PENDING  新记录与已定值冲突，重新进入待消解

====================== 裁决规则（竞争制，自定，理由附后） ======================
每条规则 = (规则优先级, 上下文条件, 提名函数)。裁决时评估全部规则，
凡上下文条件满足者参与竞争并各自提名，规则优先级最高者胜出。

  R1 优先级100 唯一最高优先级   条件：存在唯一最大记录优先级
     理由：显式优先级是来源自己声明的最强信号，唯一最高时最无争议。
  R2 优先级 90 可信来源优先     条件：候选中存在 TRUST 声明的可信来源
     理由：运维上常有权威源（如 master/配置中心），可信源应压过普通优先级。
  R3 优先级 80 多源多数值       条件：某值被 >=2 个不同来源给出
     理由：独立来源交叉印证的值可信度高（quorum 思想）。
  R4 优先级 10 最新到达         条件：恒真（兜底）
     理由：流式系统里“后到的覆盖先到的”是最常见的兜底语义（last-write-wins）。

====================== 错误/警告报告 ======================
  REF_UNRESOLVED   引用位置(行号/记录号) 引用了仍在待消解的冲突键
  REF_UNKNOWN      引用了不存在的键
  CONFLICT_OVERFLOW 同键冲突累积超过上限
  EQUAL_WEIGHT     同来源优先级且值不同（同权冲突，警告）
  UNDO             撤销消解（事件报告，不算错误）

用法：
  python3 conflict_resolver.py 输入文件
  cat 输入文件 | python3 conflict_resolver.py
  python3 conflict_resolver.py --demo     运行内置示例
"""

import re
import sys
from dataclasses import dataclass, field

DEFAULT_LIMIT = 4
REF_RE = re.compile(r"@([A-Za-z0-9_.\-]+)")

CLEAN, PENDING, RESOLVED = "CLEAN", "PENDING", "RESOLVED"


@dataclass
class Record:
    seq: int          # 全局记录序号（到达顺序，供“最新到达”规则）
    line: int         # 输入行号（引用位置报告用）
    key: str
    source: str
    priority: int
    value: str


@dataclass
class KeyState:
    key: str
    state: str = CLEAN
    records: list = field(default_factory=list)   # 该键的全部候选记录
    winner: Record = None                         # 裁决胜出记录
    overflow_reported: bool = False


class Resolver:
    def __init__(self):
        self.limit = DEFAULT_LIMIT
        self.trusted = set()
        self.keys = {}          # key -> KeyState
        self.records = []       # 全部记录（引用同步时扫描）
        self.seq = 0
        self.log = []           # 状态过程日志
        self.errors = []        # 错误/警告清单: (级别, 类型, 行号, 键, 详情)
        self.rules = [
            (100, "R1:唯一最高优先级", self._c_unique_top, self._s_unique_top),
            (90,  "R2:可信来源优先",   self._c_trusted,    self._s_trusted),
            (80,  "R3:多源多数值",     self._c_majority,   self._s_majority),
            (10,  "R4:最新到达(兜底)", self._c_always,     self._s_latest),
        ]

    # ---------------- 规则条件与提名 ----------------
    def _c_unique_top(self, cands):
        top = max(r.priority for r in cands)
        return sum(1 for r in cands if r.priority == top) == 1

    def _s_unique_top(self, cands):
        return max(cands, key=lambda r: r.priority)

    def _c_trusted(self, cands):
        return any(r.source in self.trusted for r in cands)

    def _s_trusted(self, cands):
        t = [r for r in cands if r.source in self.trusted]
        return max(t, key=lambda r: (r.priority, r.seq))

    def _c_majority(self, cands):
        seen = {}
        for r in cands:
            seen.setdefault(r.value, set()).add(r.source)
        return any(len(s) >= 2 for s in seen.values())

    def _s_majority(self, cands):
        seen = {}
        for r in cands:
            seen.setdefault(r.value, set()).add(r.source)
        best = max(seen, key=lambda v: len(seen[v]))
        return max((r for r in cands if r.value == best), key=lambda r: r.seq)

    def _c_always(self, cands):
        return True

    def _s_latest(self, cands):
        return max(cands, key=lambda r: r.seq)

    # ---------------- 状态过程 ----------------
    def emit(self, line, msg):
        self.log.append(f"[行{line:>3}] {msg}")

    def report(self, level, etype, line, key, detail):
        self.errors.append((level, etype, line, key, detail))

    def get(self, key):
        return self.keys.setdefault(key, KeyState(key))

    def add_record(self, line, key, source, priority, value):
        self.seq += 1
        rec = Record(self.seq, line, key, source, priority, value)
        self.records.append(rec)
        ks = self.get(key)

        # 同权冲突检查：同优先级、值不同
        for old in ks.records:
            if old.priority == priority and old.value != value:
                self.report("警告", "EQUAL_WEIGHT", line, key,
                            f"来源 {old.source} 与 {source} 优先级同为 {priority} "
                            f"但值不同: {old.value!r} vs {value!r}")

        ks.records.append(rec)

        if ks.state == RESOLVED and rec.value != ks.winner.value:
            ks.state = PENDING
            ks.winner = None
            self.emit(line, f"键 {key!r} 新记录与已裁决值冲突，状态 RESOLVED -> PENDING（重新待消解）")
        elif len(ks.records) == 1:
            self.emit(line, f"记录#{rec.seq} 键 {key!r} 值 {value!r}（来源 {source}, 优先级 {priority}）状态 CLEAN")
        else:
            if ks.state != PENDING:
                self.emit(line, f"键 {key!r} 出现多条记录，状态 {ks.state} -> PENDING（冲突待消解）")
                ks.state = PENDING
            self.emit(line, f"记录#{rec.seq} 键 {key!r} 累积候选 {len(ks.records)} 条")

        # 冲突累积超限
        if ks.state == PENDING and len(ks.records) > self.limit and not ks.overflow_reported:
            ks.overflow_reported = True
            self.report("错误", "CONFLICT_OVERFLOW", line, key,
                        f"同键冲突累积 {len(ks.records)} 条，超过上限 {self.limit}")

        # 处理该记录值中的 @引用
        self.check_refs(rec)

    def check_refs(self, rec):
        for ref in REF_RE.findall(rec.value):
            if ref not in self.keys:
                self.report("错误", "REF_UNKNOWN", rec.line, ref,
                            f"记录#{rec.seq}(行{rec.line}) 引用了不存在的键 @{ref}")
            elif self.keys[ref].state == PENDING:
                self.report("错误", "REF_UNRESOLVED", rec.line, ref,
                            f"记录#{rec.seq}(行{rec.line}) 引用了待消解冲突键 @{ref}")
            elif self.keys[ref].state == RESOLVED:
                # 引用已裁决键：立即同步
                rec.value = rec.value.replace("@" + ref, self.keys[ref].winner.value)
                self.emit(rec.line, f"记录#{rec.seq} 引用 @{ref} 同步为 {self.keys[ref].winner.value!r}")

    def resolve(self, line, key):
        ks = self.get(key)
        if ks.state != PENDING:
            self.emit(line, f"RESOLVE {key!r}：当前状态 {ks.state}，无需裁决")
            return
        self.emit(line, f"键 {key!r} 开始规则竞争裁决（候选 {len(ks.records)} 条）")
        cands = ks.records
        applicable = []
        for prio, name, cond, select in self.rules:
            if cond(cands):
                win = select(cands)
                applicable.append((prio, name, win))
                self.emit(line, f"  竞争 {name} -> 提名 值 {win.value!r}"
                                f"(来源 {win.source}, 记录#{win.seq})")
            else:
                self.emit(line, f"  跳过 {name}（上下文条件不满足）")
        applicable.sort(key=lambda x: -x[0])
        prio, name, win = applicable[0]
        ks.winner = win
        ks.state = RESOLVED
        ks.overflow_reported = False
        self.emit(line, f"  裁决结果：{name} 胜出，键 {key!r} 定值 {win.value!r}，"
                        f"状态 PENDING -> RESOLVED")
        self.sync_refs(line, key, win.value)

    def sync_refs(self, line, key, value):
        """裁决后，同步更新所有引用该键的记录。"""
        token = "@" + key
        for rec in self.records:
            if token in rec.value:
                old = rec.value
                rec.value = rec.value.replace(token, value)
                self.emit(line, f"  引用同步：记录#{rec.seq}(行{rec.line}) "
                                f"{old!r} -> {rec.value!r}")

    def undo(self, line, key):
        ks = self.get(key)
        if ks.state != RESOLVED:
            self.emit(line, f"UNDO {key!r}：当前状态 {ks.state}，无可撤销的裁决")
            return
        ks.state = PENDING
        ks.winner = None
        self.emit(line, f"撤销消解：键 {key!r} 状态 RESOLVED -> PENDING（回到待消解）")
        self.report("事件", "UNDO", line, key,
                    f"消解被撤销，{len(ks.records)} 条候选重新待裁决")

    # ---------------- 输入驱动 ----------------
    def run_line(self, line_no, text):
        text = text.strip()
        if not text or text.startswith("#"):
            return
        parts = text.split()
        cmd, args = parts[0].upper(), parts[1:]
        try:
            if cmd == "REC":
                key, source, prio = args[0], args[1], int(args[2])
                value = " ".join(args[3:])
                self.add_record(line_no, key, source, prio, value)
            elif cmd == "RESOLVE":
                self.resolve(line_no, args[0])
            elif cmd == "UNDO":
                self.undo(line_no, args[0])
            elif cmd == "LIMIT":
                self.limit = int(args[0])
            elif cmd == "TRUST":
                self.trusted.add(args[0])
                self.emit(line_no, f"来源 {args[0]!r} 标记为可信")
            else:
                self.report("错误", "BAD_CMD", line_no, "-", f"未知指令 {cmd!r}")
        except (IndexError, ValueError) as e:
            self.report("错误", "BAD_LINE", line_no, "-", f"行解析失败: {text!r} ({e})")

    # ---------------- 输出 ----------------
    def output(self, out=sys.stdout):
        w = out.write
        w("========== 冲突状态过程 ==========\n")
        for entry in self.log:
            w(entry + "\n")
        w("\n========== 最终键状态 ==========\n")
        for key in sorted(self.keys):
            ks = self.keys[key]
            if ks.state == RESOLVED:
                w(f"  {key:<12} RESOLVED  值={ks.winner.value!r}"
                  f"（来源 {ks.winner.source}, 记录#{ks.winner.seq}）\n")
            elif ks.state == PENDING:
                vals = {r.value for r in ks.records}
                w(f"  {key:<12} PENDING   候选{len(ks.records)}条 冲突值={sorted(vals)}\n")
            else:
                w(f"  {key:<12} CLEAN     值={ks.records[0].value!r}\n")
        w("\n========== 错误与警告清单 ==========\n")
        if not self.errors:
            w("  （无）\n")
        for level, etype, line, key, detail in self.errors:
            w(f"  [{level}] {etype:<18} 行{line:<3} 键={key:<10} {detail}\n")


DEMO_INPUT = """\
# ===== 示例输入 =====
LIMIT 3
TRUST master
# 1) 普通记录，无冲突
REC site.title cms 10 我的站点
# 2) 同键冲突：进入待消解；其中 db 与 file 同权不同值 -> 同权冲突警告
REC theme.color db    50 blue
REC theme.color file  50 green
REC theme.color api   70 red
# 3) 冲突未裁决前引用该键 -> REF_UNRESOLVED 报告（含引用位置）
REC home.banner web   10 背景色:@theme.color
# 4) 规则竞争裁决：R1(唯一最高优先级 api/70) 胜出
RESOLVE theme.color
# 5) 裁决后新引用立即同步
REC page.footer web   10 页脚配色 @theme.color
# 6) 撤销消解 -> 回到待消解并报告
UNDO theme.color
# 7) 撤销后再引用 -> 再次报告未消解
REC page.header web   10 头部配色 @theme.color
# 8) 重新裁决：仍由 R1 胜出
RESOLVE theme.color
# 9) 同键冲突持续累积超限 -> CONFLICT_OVERFLOW
REC retry.count a 1 3
REC retry.count b 1 5
REC retry.count c 1 8
REC retry.count d 1 13
# 10) 引用不存在的键 -> REF_UNKNOWN
REC bad.ref x 1 见 @no.such.key
"""


def main(argv):
    resolver = Resolver()
    if "--demo" in argv:
        lines = DEMO_INPUT.splitlines()
    elif len(argv) > 1:
        with open(argv[1], encoding="utf-8") as f:
            lines = f.read().splitlines()
    else:
        lines = sys.stdin.read().splitlines()
    for i, text in enumerate(lines, 1):
        resolver.run_line(i, text)
    resolver.output()


if __name__ == "__main__":
    main(sys.argv)
