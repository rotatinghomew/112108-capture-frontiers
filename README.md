# 连续采集的双通道追帧入湖

成像台、触发器与补拍方的本地消息，消息记录不含实拍图像或实际设备。

## 资料

来源记录使用 UTF-8 JSON Lines，每行有交付次序 `step`、记录类型 `kind` 和载荷 `body`。按交付次序阅读 `data/exchanges.jsonl`，字段说明见 `data/字典.md`。

## 校验原件

使用 Python 3.11。在项目根目录运行：

```sh
python3 -m unittest discover -s tests -v
```
