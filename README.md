# 连续采集的双通道追帧入湖

成像台、触发盒与补拍现场的本地消息湖仓。消息记录只包含元数据，不含实拍图像。

管道用 Python 3.11 标准库实现：

- 按触发盒 `boot + seq` 识别一次触发，通道再细化为 `boot/seq/camera`。
- 用硬件脉冲、相机 `clock.rev` 转换后的可靠 tick 时间、容差和通道共同判定候选。
- 在整个候选二分图中求唯一强制配对；共享帧、多候选和无法唯一裁决的关系输出冲突，不按读到顺序或超时强分。
- 重传按 `frame.id + rev` 识别；`upload` 只作为上传号别名，不参与身份或去重。
- 缺帧必须同时具备：触发盒 `close` 凭据、`1..last` 业务序号连续、当前 `clock_rev` 对应的 `coverage` 完整覆盖候选时间边界。
- 每个 `boot/seq/channel` 只有一个稳定补拍意向 `RT:<target>`，重放不会生成新单。
- 现场回执 `receipt` 幂等；`accepted` / `completed` 记录不可抹除。
- 时钟订正导致帧跨触发移动时，同一事务更新两端；未接手意向撤销，已接手/完成意向保留并追加可区分事件。
- `retake_of` 帧始终是补拍帧，不计入原班次原图。
- SQLite 保存原始消息、状态、变更事件和读取端游标；新班次使用独立 `--shift` 数据库即可独立重跑。

## 运行

```sh
python3 -m unittest discover -s tests -v

# 入库并查看值班员视图
python3 pipeline.py --shift g9 ingest data/exchanges.jsonl
python3 pipeline.py --shift g9 status

# JSON 视图
python3 pipeline.py --shift g9 status --json

# 持续轮询一个或多个 JSONL 来源；重复读取是幂等的
python3 pipeline.py --shift g9 watch data/exchanges.jsonl --interval 1
```

默认数据库为 `.imaging-lake/<shift>.sqlite`；也可用 `--db /path/to/lake.sqlite` 指定。

## 现场补拍 API/CLI

可领取意向：

```sh
python3 pipeline.py --db .imaging-lake/g9.sqlite claimable
python3 pipeline.py --db .imaging-lake/g9.sqlite claim g9/2/cam2 --claimer team-a
python3 pipeline.py --db .imaging-lake/g9.sqlite receipt g9/2/cam2 \
  --receipt o11 --state accepted
python3 pipeline.py --db .imaging-lake/g9.sqlite receipt g9/2/cam2 \
  --receipt o12 --state completed
```

`claim` 只建立领用租约，不等同于接手回执；没有 `accepted/completed` 回执前，若原帧归位或候选变为冲突，意向仍会被撤销。同一领取人重试领取是幂等的。

## 变更读取与崩溃恢复

```sh
python3 pipeline.py --db .imaging-lake/g9.sqlite changes --consumer screen-1
python3 pipeline.py --db .imaging-lake/g9.sqlite ack --consumer screen-1 --seq 12
```

Python API：

```python
from imaging_lake import LakeStore, replay

engine = replay(rows, shift="g9")
status = engine.operator_view()

with LakeStore(".imaging-lake/g9.sqlite", shift="g9") as lake:
    lake.ingest_jsonl("data/exchanges.jsonl")
    for event in lake.poll_changes("field-reader"):
        # 用 event["event_id"] 或 event["intention_id"] 幂等落库。
        ...
    lake.ack("field-reader", event["seq"])
```

读取端未 ack 就退出时，下次会从旧游标重新读到事件。事件可能 at-least-once 重投，但 `intention_id` 永远稳定，`intention.issued` 只会有一次逻辑建单；撤销、挂起、恢复和回执分别是不同事件类型。`LakeStore.deliver(consumer, handler)` 还提供 `processed_events` 幂等表。

SQLite 文件中可直接查询：

- `raw_records`：原始 JSONL 消息；
- `trigger_facts`：当前触发/通道事实投影；
- `retake_intentions`：稳定补拍单投影；
- `outbox_events`：按单调 `seq` 排列的变更事件；
- `consumer_offsets` / `processed_events`：读取端游标与幂等处理记录。

## Python 领域 API

```python
from imaging_lake import LakeEngine, replay

engine = replay(rows)
engine.facts                 # 每个 boot/seq/channel 的当前事实和覆盖证据
engine.conflicts             # 无法唯一裁决的完整候选集合
engine.intentions            # 稳定补拍单、回执、历史、撤销/保留标记
engine.events                # 有序变更日志
engine.operator_view()       # 值班员汇总
```

也可以直接使用根目录兼容入口：

```python
from pipeline import LakeStore, replay
```

## 值班员视图包含什么

`operator_view()` / `status` 输出：

- `usable_images`：已唯一配对、当前能用的原图。
- `ambiguous_attributions` / `conflicts`：有候选但归属不能唯一裁决的关系。
- `unconfirmed_gaps`：凭据、序号或覆盖尚不足的缺口。
- `confirmed_missing`：两路证据齐全后确认的缺帧，并带补拍单状态。
- `claimable_retakes`：当前可领取的稳定补拍意向。
- `retake_intentions` / `handed_to_retake_team`：全部补拍单、回执和处理历史。
- `retake_frames`：带 `retake_of` 的补拍取得帧，不混入 `original_frames`。
- `coverage_for_each_decision`：每个判定使用的触发盒序号覆盖与相机统一时刻覆盖，包括覆盖承诺 ID、`clock_rev`、区间和不支持原因。

## 样例资料的最终结论

重放 `data/exchanges.jsonl` 后：

| 目标 | 原图事实 | 补拍/回执 |
|---|---|---|
| `g9/1/cam1` | `f2` 经 `clock.rev=2` 从 `107` 订正为 `103`，唯一配对 | `o8 accepted` 已存在；原帧后到，接手记录和变更历史保留 |
| `g9/1/cam2` | 无原图 | `o9 accepted`、`o10 completed`；`rep4` 是补拍帧 |
| `g9/2/cam2` | 覆盖完整但无有效 `q` 脉冲帧，确认缺帧 | 可领取 |
| `g9/3/cam1` | `f2` 订正移走后确认缺帧 | 可领取 |
| `g9/3/cam2` | 覆盖完整但无原图，确认缺帧 | 可领取 |

## 校验原件

```sh
python3 -m unittest discover -s tests -v
```

`tests/test_exchanges.py` 仍只校验来源交换件；`tests/test_pipeline.py` 覆盖上述追帧、冲突、覆盖、补拍单和恢复规则。
