#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
conflict_resolver.py — 多来源记录键冲突状态机(纯 Python 标准库, 单文件)

用法:
    python3 conflict_resolver.py --demo            # 运行内置示例
    python3 conflict_resolver.py < input.txt       # 从标准输入读取记录流
    python3 conflict_resolver.py --max-pending 3   # 调整冲突累积上限

────────────────────────────────────────────────────────────
输入格式(行式 DSL, 记录逐条流入; 空行与 # 开头的行忽略):
    R <键> <来源> <值...> <优先级>   追加一条记录; 值中 @键 表示引用其他键
    A <键>                          对指定键发起消解裁决(规则竞争)
    U <键>                          撤销该键的消解结果, 回到待消解状态

格式设计理由:
    记录是"流", 行式 DSL 让每条记录天然占一行, 可增量逐行解析, 无需整体
    读入; 字段位置固定(优先级恒为最后一个 token, 值可含空格与 @引用),
    无引号/转义负担, 比 CSV 易读、比 JSONL 简短, 人写机器读都方便。

状态机(每个键独立):
    CLEAN ──值不一致的新记录──> PENDING(待消解, 冲突持续累积)
    PENDING ──A 裁决(规则竞争)──> RESOLVED(键值确定, 引用同步更新)
    RESOLVED ──U 撤销 / 值不同的新记录──> PENDING(并报告)

裁决规则(按规则优先级从高到低竞争, 条件满足的最高优先级规则胜出):
    1. unique_top_priority (100): 来源优先级唯一最高者胜出。
       条件: 候选中最高优先级只出现一次。来源可信度是最强信号, 优先采用。
    2. majority_value (80): 某取值占候选严格多数时, 取该取值中
       来源优先级最高(并列取最新)的候选。来源无关的共识信号, 可打破同权僵局。
    3. latest_arrival (10): 最新到达者胜出, 条件恒真。
       兜底规则, 保证裁决必定终止且结果确定(可全序)。
────────────────────────────────────────────────────────────
"""

import sys
from collections import Counter
from dataclasses import dataclass

CLEAN, PENDING, RESOLVED = "CLEAN", "PENDING", "RESOLVED"
MAX_PENDING_DEFAULT = 4  # 同键冲突累积上限(超过即报告)


@dataclass
class Candidate:
    seq: int
    source: str
    value: str
    priority: int


@dataclass
class Record:
    seq: int
    key: str
    value: str  # 原始值(可能含 @引用)


@dataclass
class Rule:
    name: str
    priority: int
    reason: str
    condition: object  # callable([Candidate]) -> bool
    pick: object       # callable([Candidate]) -> Candidate

    def applicable(self, cands):
        return self.condition(cands)


def _top_unique(cands):
    top = max(c.priority for c in cands)
    return sum(c.priority == top for c in cands) == 1


def _pick_top(cands):
    return max(cands, key=lambda c: (c.priority, c.seq))


def _majority_value(cands):
    _, n = Counter(c.value for c in cands).most_common(1)[0]
    return n > len(cands) / 2


def _pick_majority(cands):
    value, _ = Counter(c.value for c in cands).most_common(1)[0]
    return max((c for c in cands if c.value == value),
               key=lambda c: (c.priority, c.seq))


RULES = sorted([
    Rule("unique_top_priority", 100,
         "来源可信度唯一最高, 信任信号最强",
         _top_unique, _pick_top),
    Rule("majority_value", 80,
         "取值获严格多数共识, 可打破同权僵局",
         _majority_value, _pick_majority),
    Rule("latest_arrival", 10,
         "兜底: 最新到达者胜出, 保证裁决可终止",
         lambda c: True, lambda c: max(c, key=lambda x: x.seq)),
], key=lambda r: -r.priority)


class Engine:
    def __init__(self, max_pending=MAX_PENDING_DEFAULT):
        self.max_pending = max_pending
        self.seq = 0
        self.candidates = {}     # key -> [Candidate]
        self.state = {}          # key -> CLEAN/PENDING/RESOLVED
        self.resolved = {}       # key -> 已确定的原始值
        self.records = []        # [Record] 全部流入记录(用于引用同步)
        self.materialized = {}   # 记录序号 -> 引用替换后的物化值
        self.events = []         # 状态过程
        self.reports = []        # (行号, 类别, 消息)

    # ---------- 基础 ----------
    def report(self, ln, kind, msg):
        self.reports.append((ln, kind, msg))

    def resolve_value(self, value, ln=None, report=False, seen=()):
        """把值中的 @键 替换为该键当前已确定的值(递归, 带环保护)。"""
        out = []
        for tok in value.split():
            if tok.startswith("@") and len(tok) > 1:
                k = tok[1:]
                st = self.state.get(k)
                if st is None:
                    if report:
                        self.report(ln, "REF_UNKNOWN",
                                    f"引用未知键: 引用位置=行{ln} 键={k}")
                    out.append(tok)
                elif st == PENDING:
                    if report:
                        self.report(ln, "REF_PENDING",
                                    f"引用待消解键: 引用位置=行{ln} 冲突键={k}")
                    out.append(tok)
                elif k in seen:
                    out.append(tok)  # 引用环, 保持原样
                else:
                    out.append(self.resolve_value(
                        self.resolved[k], ln, report, seen + (k,)))
            else:
                out.append(tok)
        return " ".join(out)

    def _rematerialize(self, ln, cause):
        """键值变化后, 重算所有记录的物化值并记录同步事件。"""
        for rec in self.records:
            new = self.resolve_value(rec.value, report=False)
            old = self.materialized.get(rec.seq)
            if old != new:
                self.materialized[rec.seq] = new
                self.events.append(
                    f"  [行{ln}] SYNC 记录#{rec.seq}(键={rec.key}) 因[{cause}] "
                    f"同步更新: {old!r} -> {new!r}")

    # ---------- R: 记录流入 ----------
    def add_record(self, ln, key, source, value, priority):
        self.seq += 1
        cand = Candidate(self.seq, source, value, priority)
        cands = self.candidates.setdefault(key, [])

        # 同权冲突: 来源优先级相同且值不同
        for c in cands:
            if c.priority == priority and c.value != value:
                self.report(ln, "EQUAL_WEIGHT",
                            f"同权冲突: 键={key} 优先级同为{priority} 值不同 "
                            f"({c.value!r}@{c.source} vs {value!r}@{source})")
                break
        cands.append(cand)
        distinct = {c.value for c in cands}
        st = self.state.get(key)

        if st is None:
            self.state[key] = CLEAN
            self.resolved[key] = value
            self.events.append(f"  [行{ln}] 键 {key}: 新建 -> CLEAN 值={value!r}")
        elif st == CLEAN and len(distinct) > 1:
            self.state[key] = PENDING
            del self.resolved[key]
            self.events.append(
                f"  [行{ln}] 键 {key}: CLEAN -> PENDING 冲突形成(候选{len(cands)}条)")
        elif st == CLEAN:
            self.events.append(f"  [行{ln}] 键 {key}: 取值一致, 保持 CLEAN")
        elif st == RESOLVED and value != self.resolved.get(key):
            del self.resolved[key]
            self.state[key] = PENDING
            self.events.append(
                f"  [行{ln}] 键 {key}: RESOLVED -> PENDING "
                f"新记录与已决值不同, 冲突重开")
            self._rematerialize(ln, f"键{key}冲突重开")
        elif st == PENDING:
            self.events.append(
                f"  [行{ln}] 键 {key}: 冲突累积, 候选{len(cands)}条")

        if self.state[key] == PENDING and len(cands) > self.max_pending:
            self.report(ln, "ACCUM_OVERFLOW",
                        f"冲突持续累积超上限: 键={key} 候选{len(cands)}条 "
                        f"> 上限{self.max_pending}")

        rec = Record(self.seq, key, value)
        self.records.append(rec)
        self.materialized[rec.seq] = self.resolve_value(value, ln, report=True)

    # ---------- A: 消解裁决(规则竞争) ----------
    def adjudicate(self, ln, key):
        cands = self.candidates.get(key)
        if not cands:
            self.report(ln, "NO_SUCH_KEY", f"裁决失败: 键={key} 无任何记录")
            return
        if self.state[key] == CLEAN:
            self.events.append(f"  [行{ln}] 键 {key}: 无冲突, 无需裁决")
            return
        trace, winner = [], None
        for rule in RULES:
            ok = rule.applicable(cands)
            trace.append(f"{rule.name}#{rule.priority}={'适用' if ok else '跳过'}")
            if ok:
                winner = rule.pick(cands)
                self.events.append(
                    f"  [行{ln}] 键 {key}: PENDING -> RESOLVED "
                    f"规则竞争[{' | '.join(trace)}] 胜出={rule.name} "
                    f"值={winner.value!r} 来源={winner.source}")
                break
        self.resolved[key] = winner.value
        self.state[key] = RESOLVED
        self._rematerialize(ln, f"键{key}裁决完成")

    # ---------- U: 撤销消解 ----------
    def revoke(self, ln, key):
        if self.state.get(key) != RESOLVED:
            self.report(ln, "REVOKE_NOOP",
                        f"撤销无效: 键={key} 当前非已消解状态")
            return
        old = self.resolved.pop(key)
        self.state[key] = PENDING
        self.events.append(
            f"  [行{ln}] 键 {key}: RESOLVED -> PENDING 消解被撤销(原值={old!r})")
        self.report(ln, "REVOKED",
                    f"消解撤销: 键={key} 回到待消解状态 原值={old!r}")
        self._rematerialize(ln, f"键{key}消解撤销")


def run(lines, max_pending=MAX_PENDING_DEFAULT):
    eng = Engine(max_pending)
    for ln, raw in enumerate(lines, 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        cmd = parts[0].upper()
        try:
            if cmd == "R":
                key, source = parts[1], parts[2]
                value, priority = " ".join(parts[3:-1]), int(parts[-1])
                eng.add_record(ln, key, source, value, priority)
            elif cmd == "A":
                eng.adjudicate(ln, parts[1])
            elif cmd == "U":
                eng.revoke(ln, parts[1])
            else:
                eng.report(ln, "BAD_CMD", f"未知命令: {cmd}")
        except (IndexError, ValueError):
            eng.report(ln, "BAD_FORMAT", f"格式错误: {line}")
    return eng


def render(eng):
    out = ["== 冲突状态过程 =="]
    out += eng.events or ["  (无事件)"]
    out += ["", "== 错误与报告清单 =="]
    if eng.reports:
        out += [f"  [行{ln}] {kind}: {msg}" for ln, kind, msg in eng.reports]
    else:
        out += ["  (无)"]
    out += ["", "== 最终键值表 =="]
    for key in sorted(eng.candidates):
        st = eng.state[key]
        raw = eng.resolved.get(key)
        val = eng.resolve_value(raw, report=False) if raw is not None else "<待消解>"
        cands = eng.candidates[key]
        srcs = ",".join(sorted({c.source for c in cands}))
        out.append(f"  {key:<8} 状态={st:<8} 值={val:<24} "
                   f"候选={len(cands)} 来源=[{srcs}]")
    refs = [r for r in eng.records if "@" in r.value]
    if refs:
        out += ["", "== 引用记录最终物化值 =="]
        for r in refs:
            out.append(f"  记录#{r.seq} 键={r.key} 原始={r.value!r} "
                       f"物化={eng.materialized[r.seq]!r}")
    return "\n".join(out)


DEMO = """\
# ── 场景1: host 冲突形成 / 引用待消解 / 累积超上限 / 规则1裁决 / 撤销 ──
R host  web1 10.0.0.1 5
R host  web2 10.0.0.2 5
R alias app1 @host 1
R host  web1 10.0.0.1 5
R host  web3 10.0.0.3 7
R host  web2 10.0.0.2 5
A host
R alias2 app2 @host 1
U host
R alias3 app3 @host 1
A host
# ── 场景2: mode 同权冲突, 规则1/2均不适用, 兜底规则3(最新)胜出 ──
R mode  jobA on 1
R mode  jobB off 1
A mode
# ── 场景3: color 同权但取值有多数共识, 规则2(majority)胜出 ──
R color s1 red 3
R color s2 blue 3
R color s3 red 3
A color
"""


def main(argv):
    args = argv[1:]
    max_pending = MAX_PENDING_DEFAULT
    if "--max-pending" in args:
        i = args.index("--max-pending")
        max_pending = int(args[i + 1])
        del args[i:i + 2]
    if "--demo" in args or (not args and sys.stdin.isatty()):
        lines = DEMO.strip().splitlines()
        print("== 输入(内置示例) ==")
        print("\n".join(f"  {i:>2}: {l}" for i, l in enumerate(lines, 1)))
        print()
    else:
        lines = sys.stdin.read().splitlines()
    print(render(run(lines, max_pending)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
