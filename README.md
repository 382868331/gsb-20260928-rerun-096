# 服务发现运行核心（第一轮）

纯内存、无外部注册中心依赖的服务发现核心，作为后续调用框架的本地地址来源。

## 能力

- **虚拟实例登记**：`(service, endpoint)` 为一个槽位；每次 `Register` 分配随机、短生命周期的 `Identity`，并返回租期凭证（TTL、截止时间、版本）。
- **租期**：`Heartbeat(identity)` 仅能续当前身份的租约。进程重启后重新登记得到新身份；旧身份的心跳与主动退出都返回 `ErrLeaseNotFound`，无法续上或顶替新租约。
- **失效与主动退出分离**：超时记为 `KindExpired`，主动退出记为 `KindDeregistered`，重启抢占同槽位记为 `KindSuperseded`；`Stats()` 分别计数。
- **版本订阅**：所有变更写入严格单调、连续的版本日志。`Poll(fromVersion)` 从指定版本（不含）拉取增量；落后于保留窗口或首次订阅（`fromVersion == 0`）时返回完整 `Snapshot`，保证不静默丢事件。`Watch(ctx, fromVersion)` 提供阻塞流式订阅，重连时用最后处理完的版本续传；慢消费者落后到窗口外会自动收到全量快照再继续。
- **可控时钟**：`Clock` 接口 + `FakeClock.Advance` 用于确定性测试并发续租、重连与重启；生产用 `RealClock` 与后台 janitor 扫描过期租约。

## 关键 API

```go
reg := discovery.New()
lease, _ := reg.Register(discovery.RegisterInput{
    Service: "checkout", Endpoint: "10.0.0.7:9000", TTL: 5 * time.Second,
})
deadline, _ := reg.Heartbeat(lease.Instance.Identity)
_ = reg.Deregister(lease.Instance.Identity)

instances := reg.Lookup("checkout")
snap, delta, currentVersion, _ := reg.Poll(lastSeenVersion)
updates, _ := reg.Watch(ctx, lastSeenVersion)
```

事件版本约定：

- 版本从 1 开始连续递增，快照携带其对应的目录版本。
- 收到快照后以 `Snapshot.Version` 为新游标；事件只处理 `Version > cursor`。
- `Poll` 拒绝超前版本（返回 `ErrInvalidArgument`），不会回假数据。

## 测试

`go test -race ./...` 覆盖：登记与查询、并发续租保活、旧心跳 fencing、重启槽位一致性、失效/主动退出分别记账、增量续传、落后全量快照、断连重连无缺口、Watch 压缩后自动快照、假时钟 janitor 过期。
