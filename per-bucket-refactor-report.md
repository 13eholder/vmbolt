# Per-Bucket 纯内存模型重构报告

> 分支：`pure-memory-node`
> 日期：2026-06-19
> 范围：把「单棵全局树 + 根目录 bucket」改为「每 Bucket 一棵独立可原子发布的树」，Nid 改为 `BucketId(高16) | NodeId(低48)` 且 NodeId 可回收。

---

## 1. 背景与目标

重构前的 `pure-memory-node` 把**所有 bucket 的节点**塞进**一个**全局 `dbState{ nodes map[Nid]*snapNode }`，由单个 `atomic.Pointer[dbState]` 在每次提交时整体替换；顶层 bucket 以 KV 条目（`name → 编码后的 InBucket`，`BucketLeafFlag`）存在一个合成的**根目录 bucket** 里；`Nid` 是扁平的 64 位单调计数器（`memMeta.nextNid`），永不回收。

本次重构目标（与用户确认的 4 项决策）：

1. **保留 `db.Update(fn)` + 全局单写锁**；`Commit` 逐个发布被改动的 bucket，**不保证跨 bucket 原子性**。
2. **仅 per-bucket 读快照**：读事务在首次访问某 bucket 时钉住当时那一代；A、B 可处于不同代。
3. **砍掉全部磁盘时代残留**：保留 Nid 0/1/2、`BucketLeafFlag`、`encode/decodeBucketMeta`、根目录 bucket；Bucket 目录 = `map[string]*bucketHandle`；16 位 `BucketId` 创建时分配、删除时回收。
4. 每个 bucket 用 `atomic.Pointer[bucketState]` 包裹 `{root, nodes, sequence}`；writer-private 的 `nextNodeId` + LIFO `freelist` 挂在 handle 上（全局单写锁下无需克隆/发布 freelist）。
5. **`tx_check` 本轮跳过**（改为 no-op stub，留作后续）。

预期收益：bucket 之间真正互不干扰；未触及的 bucket 在提交时零开销（不重建、不重发）；node id 按 bucket 回收。

---

## 2. 目标数据模型

```go
// internal/common/nid.go   —— Nid 布局
const (
    NidBucketShift = 48
    NidNodeMask    = (1 << 48) - 1
)
type BucketId uint16                                  // BucketId 0 保留（与 Nid 0「未分配」哨兵冲突），分配从 1 起
func MakeNid(b BucketId, node uint64) Nid             // 高16 | 低48
func (n Nid) BucketOf() BucketId                      // 高16
func (n Nid) NodeId() uint64                          // 低48
```

```go
// bucket_state.go   —— per-bucket 状态
type bucketState struct {                             // 不可变、已发布的「一代」
    id       common.BucketId
    root     common.Nid                               // 根节点全 Nid
    nodes    map[common.Nid]*snapNode                 // 本 bucket 的 B+tree
    sequence uint64
}

type bucketHandle struct {                            // DB.buckets[name] 里的长生命周期句柄
    id   common.BucketId
    name string
    state atomic.Pointer[bucketState]                 // 锁无关读
    nextNodeId  uint64                                // writer-private 低48分配器
    freeNodeIds []uint64                              // LIFO 可回收 NodeId 栈
}
```

```go
// db.go   —— DB 现在持有一个目录而非全局快照
type DB struct {
    bucketsMu     sync.RWMutex
    buckets       map[string]*bucketHandle
    txid          atomic.Uint64                        // 全局单调事务号（Tx.ID）
    nextBucketId  uint16                               // writer-private 16位 BucketId 分配器
    freeBucketIds []uint16                             // LIFO
    rwlock        sync.Mutex                           // 单全局写者（保留）
    // ...
}
```

```go
// bucket.go / tx.go   —— Bucket 是事务本地的 per-bucket 句柄
type Bucket struct {
    tx *Tx; name string; handle *bucketHandle
    base     *bucketState                             // 钉住的那一代：读视图 / COW 基线
    rootNode *workNode                                // 物化的可变根
    dirty    map[common.Nid]*workNode                 // 事务本地可变节点缓存（仅写事务）
    obsolete map[common.Nid]struct{}                  // 本事务释放的 Nid
    sequence uint64                                   // 事务本地可变 sequence
    snapNextNode uint64; snapFreeIds []uint64         // 回滚快照
    FillPercent float64
}

type Tx struct {
    writable bool; managed bool; db *DB; id common.Txid
    bctx     map[string]*Bucket                       // 懒加载的 per-bucket 上下文
    created  map[string]*bucketHandle                 // 待提交的 CreateBucket
    deleted  map[string]struct{}                      // 待提交的 DeleteBucket
    stats TxStats; commitHandlers []func()
}
```

`snapNode` / `workNode` / `materializeWorkNode` / `freeze` / `readNodeView`（`snapshot_node.go`）与**整棵 B+tree 引擎**（`node.go` 的 split/finalize/rebalance）**完全未改** —— bucket 无关。只是把它们的容器从「全局」搬到「per-bucket」。

---

## 3. 关键流程

### 提交（`Tx.Commit`）—— build-all-then-publish-all
1. 对每个 touched bucket：`rebalance()` → `spill()`（`finalize` 经 `bucket.allocate()` 分配 Nid、记录 obsolete）→ 组装新的 `bucketState`（clean 节点按指针共享 + freeze 脏 workNode + 删 obsolete）。
2. **先全部构造完，再统一发布**：`bucketsMu.Lock()` 下，created 的 handle 入 `db.buckets`，每个 `handle.state.Store(newState)`，deleted 的从 map 移除并 `freeBucketId`。
3. `close()`、commitHandlers。

> 这样保证「构造失败 → 什么都不发布 → 干净回滚」。跨 bucket 的可见性窗口（逐个 Store 之间）被接受（per-bucket 快照）。

### 回滚（`Tx.rollback`）
仅写事务回滚分配器状态（`nextNodeId`/`freeNodeIds` 从快照还原）、回收 created-then-rolled-back 的 `BucketId`、把脏 workNode 壳还池。

### 读（`Tx.Bucket(name)`）
读事务懒加载：首次访问时 `base = handle.state.Load()` 钉住当前代并缓存进 `tx.bctx`；后续同一 bucket 复用同一 `base`，遍历期间该 bucket 代不变。不同 bucket 可能不同代。

### 节点 id 分配（`bucketHandle.allocNode`）
优先复用 `freeNodeIds` 栈顶（LIFO），否则 `nextNodeId++`。返回的 Nid 恒带本 bucket 的高 16 位前缀。

---

## 4. 实现分阶段

| 阶段 | 内容 | 状态 |
|------|------|------|
| 1 | `internal/common/nid.go`（Nid 拆位 + `BucketId`）；新 `bucket_state.go`（`bucketState`/`bucketHandle`/`allocNode`/`freeNode`） | ✅ |
| 2–5 | 协调重写 `db.go`（目录 map、删 `dbState`/`memMeta`/`loadState`/`allocate`、`init` 空目录、`allocBucketId`/`freeBucketId`）、`tx.go`（`bctx`/`created`/`deleted`、per-bucket 提交、回滚还原）、`bucket.go`（删 `InBucket`/根目录/`BucketLeafFlag`；per-bucket `node`/`nodeForWrite`/`allocate`/`spill`/`rebalance`/`free`）、`node.go` 接线（`finalize` 用 `b.allocate()`、`rebalance` 用 `b.dirty`/`b.dropNode`） | ✅ |
| 6 | `internal/common` 清理：`InBucket`/`Meta` 保留（cmd/surgeon/guts_cli 兼容层仍用），bbolt 包不再引用；`internal/freelist` 不动（旧的 no-op 兼容；新 freelist 是 handle 上的 `[]uint64`） | ✅ |
| 7 | `tx_check.go` 改 no-op stub（返回无错误，免得 `btesting.MustCheck` `os.Exit`） | ✅（后续补真实检查） |
| 8 | 测试与基准：改 3 个旧内部测试（`allocate_test`→freelist 回收测试、`node_test` 修 setup、`db_whitebox` 修 `db.state`/`Page`），`tx_test` 的 `TestTx_Cursor`→`ForEach`、`TestTx_releaseRange` skip；新增 `perbucket_sanity_test`、`debug_split_test`（回归） | ✅ |

---

## 5. 调试中发现并修复的 3 个非显然正确性 bug

### Bug 1：根节点被 freeze 两次 → 读出空值
- **现象**：新建 bucket 写入后读回为空（`Get` 返回 `""`）。
- **根因**：旧代码里根占位符在 `b.nodes[0]`（per-bucket 缓存），而冻结的脏节点在 `tx.dirtyNodes[pgid]`（**两个独立 map**）。我把它们合并成**一个** `b.dirty` 后，根同时以键 `0`（占位）和真实 Nid `X` 存在 → `buildPublishedState` 遍历 `dirty` 时同一个 workNode 被 `freeze()` 两次，第二次拿到 `inodes=nil`（第一次已转移所有权）。
- **修**：`finalize` 给节点分配真实 Nid 时 `delete(n.bucket.dirty, 0)` 去掉占位键，确保每个 workNode 只冻结一次。

### Bug 2：`close()` 无条件回滚分配器 → 子节点 Nid 重复
- **现象**：对一个**已有提交数据**的 bucket 追加大批 key 触发 split 后，`Get` 丢失一整个叶子的 key；dump 发现 branch 的相邻两条目指向**同一个子 Nid**（`[0,0,1,2,…]` 错位）。
- **根因**：我把 alloc 状态还原放在 `close()` 里，而 `close()` 在 **commit 成功后也调用** → 把已发布提交里推进过的 `nextNodeId` 还原成快照旧值 → 下一次提交从旧值重新分配 → 子节点 Nid 与上一代重复。
- **修**：把还原逻辑从 `close()` 移到 `rollback()`；commit 走的 `close()` 不动分配器。

### Bug 3：读事务的 Rollback 把 `nextNodeId` 清零
- **现象**：sanity 测试里，只要中间夹一个 `db.View`，后续 split 就触发 Bug 2 的同款错位（`before big-insert: nextNodeId=0`）。
- **根因**：`db.View` **无论成功与否都会 `Rollback()`** → `rollback()` 遍历 `tx.bctx` 还原 `handle.nextNodeId = b.snapNextNode`。但**读事务从不快照** `snapNextNode`（零值）→ 把真实计数器写成 0。
- **修**：还原分配器状态加 `if tx.writable` 守卫；读事务的回滚不碰 handle。

---

## 6. 有意的语义变更（需知晓）

- **读快照是惰性 per-bucket**：读事务在首次访问某 bucket 时钉住当时那一代，**不是 Begin 时刻全局快照**。`TestTx_releaseRange` 因此 skip（它测的是旧的 eager 快照 + freelist.pending 释放机制，二者均已不存在）。
- **`Tx.Check` 是 no-op stub**：返回无错误（vacuously 通过）。后续如需，可实现 per-bucket 的可达性/键序检查。
- **Bucket 级嵌套 API 是 stub**（返回 `ErrNestedBucketsUnsupported`）：`cmd/bbolt` 的嵌套 bucket 场景行为与重构前一致（本就不支持）；顶层 bucket 场景正常。
- **`BucketId` 16 位 → 最多 65535 并发 bucket**：创建/删除会回收 id，所以这是**并发**上限，不是累计上限。超限 `allocBucketId` 会 panic（硬护栏）。
- **`tx.Cursor()` 改为有序目录游标**：无根 bucket 树，故 `tx.Cursor()` 现返回一个**按名排序**遍历顶层 bucket 的 directory cursor（见 §10.1）；bucket 内 KV 遍历用 `tx.Bucket(name).Cursor()`。`Tx.Inspect()` 保留（聚合所有顶层 bucket）。
- **`Tx.Page()` 返回 not-supported**：node id 已 bucket 化，全局 page-id 视图无意义。
- **`tx.ForEach` 无序、`tx.Cursor()` 有序**：目录是 `map[string]`，`ForEach` 顺序未定义；需要确定性（Hash/快照/defrag）的有序枚举用 `tx.Cursor()`（按名排序）。

---

## 7. 验证

```
go build ./...        ✅
go vet ./...          ✅（无诊断）
go test ./...         ✅ 全绿（含 TestMemoryMode_* 全套、TestPerBucketSanity、TestSplitKeepsAllKeys）
go test -race ./...   ✅ 无数据竞争
基准可运行             ✅ BenchmarkPut_1KB ~71µs/op, BenchmarkGet_1KB ~3.4µs/op（与历史基线同量级）
```

新增的端到端回归测试覆盖：创建/获取/删除 bucket、跨提交 put/get/delete、强制 split（多级树）、rebalance 合并、回滚、两 bucket 互不干扰、BucketId 回收、`ForEach`、以及「读事务不应扰动 writer-private 分配器状态」（针对 Bug 3 的回归）。

---

## 8. 文件改动（核心）

新增：`bucket_state.go`、`internal/common/nid.go`、`perbucket_sanity_test.go`、`debug_split_test.go`（回归）。
重写：`db.go`、`tx.go`、`bucket.go`。
清空/改 stub：`mem_meta.go`（清空）、`tx_check.go`（no-op）。
接线：`node.go`。
测试适配：`allocate_test.go`、`node_test.go`、`db_whitebox_test.go`、`tx_test.go`、`bucket_test.go`。
**未动**：`snapshot_node.go`、`cursor.go`、`internal/freelist`、`internal/common`（仅新增 `nid.go`）。

核心 6 文件净改动：`+619 / −1012`（−393 行）。

---

## 9. 后续（out of scope）

- `tx_check` per-bucket 真实实现（可达性 + 键序检查）。
- 更新 `.docs-glm/` 架构报告与 SVG 到新的 per-bucket 模型。
- 若需要并发写不同 bucket，可叠加 per-bucket rwlock（当前仍是全局单写锁）。
- 若需要 eager Begin-time 读快照或显式 free/reclaim API，需重新评估（当前为惰性 per-bucket）。

---

## 10. etcd 后端兼容性补充（后续增量）

为支持作为 etcd 的存储后端（非持久化/内存型 etcd 定位），在 per-bucket 基础上补了 4 个口子。对照 etcd v3.6.5 `server/storage/backend/{backend.go, batch_tx.go}` 核对。

### 10.1 有序的 `tx.Cursor()` —— 顶层 bucket 目录游标
etcd 的 `Hash()`（跨副本 CRC32 一致性校验，须逐字节确定）和 `defragdb()` 都用 `tx.Cursor()` 枚举所有顶层 bucket。给 `Cursor` 加 **directory 模式**（`bucket==nil`）：`tx.Cursor()` 创建时快照 bucket 名集合（committed ∪ created − deleted）并**排序**，`First/Next/Seek/Last/Prev` 按名遍历，yield `(name, nil)`，再用 `tx.Bucket(name)` 打开对应 bucket。

> 注意：`tx.ForEach` 仍是 `map[string]` 无序遍历；需要确定性顺序用 `tx.Cursor()`。

### 10.2 BMSP 快照格式 + `WriteTo`/`Restore` —— 按需落盘、可恢复
自定义流式格式（非 bbolt 页格式，引擎本身 page-less），完全有序、长度哨兵终结（无需预计数）：
```
header(16B): "BMSP" | ver u32 | pageSize u32 | reserved u32
每 bucket(按名排序,经 tx.Cursor): nameLen u32 | name | sequence u64
  每 KV(经 ForEach,有序): keyLen u32(=0 终结本 bucket) | key | valLen u64 | val
end: nameLen u32 = 0
```
- `Tx.WriteTo`（实现 `io.WriterTo`，etcd `Snapshot.WriteTo` 直接调）/`Copy`/`CopyFile` 按 `tx.Cursor()` + `ForEach` 序列化。
- `DB.Restore(reader)` 读 BMSP 重建（一条写事务，`FillPercent=0.9` 按序回放）；`Open(path)` 若文件带 BMSP 魔数则**自动 rehydrate**，非 BMSP/缺失则空库起步。
- **不增加提交期持久化写入**：快照只在显式 `WriteTo`/`CopyFile` 时落盘；提交间数据易失（接受的取舍）。

### 10.3 `Tx.Size()` 全库估算
原来只累加本 tx 触碰过的 bucket（刚 Begin 的读事务返回 0）。改为遍历所有 bucket 当前代 `node.size()` 之和（touched 用 tx 视图，其余用已发布代）。etcd 用它做快照传输速率预估、`Size()`/`SizeInUse()` 监控。

### 10.4 Cohort —— 指定 bucket 子集的联合原子可见性
**问题**：etcd 把 `consistent_index` 存在 `meta` bucket、MVCC 树存在 `key` bucket，要求二者**同代可见**（否则快照可能抓到 `consistent_index=C` 而 key 只有到 `C'` 的数据，恢复出错）。但 per-bucket 模型默认各自独立发布，逐个 `Store` 之间有可见性窗口。

**关键收敛**：不需要让全部 bucket 原子，**只需 `meta`+`key` 这一对联合原子**，其余 bucket 仍可独立发布。

**解法**：cohort = 共享一个原子发布点的 bucket 子集。
```go
type bucketCohort struct { state atomic.Pointer[cohortSnapshot] }
type cohortSnapshot struct { members map[string]*bucketState }   // 全部成员的联合已发布态
// bucketHandle.cohort *bucketCohort   // nil = 独立（默认）
```
- **读**：cohort 成员的 base 从 tx **钉住的那一份 cohortSnapshot** 取（`pinCohort` 首次访问缓存）→ 一次读事务里所有成员永远同代。
- **提交**：touched 独立 bucket 各自 `Store`；每个 dirty cohort **重建联合快照**（被改成员用新态、未改成员按指针 carry），**一次 `Store`** → 成员整体可见/不可见。
- **API**：`db.NewCohort() *Cohort`；`tx.AssignCohort(name, c)`（不存在则建、存在则收编；同 cohort 幂等、跨 cohort 报错）。etcd 侧：`ag := db.NewCohort(); tx.AssignCohort([]byte("meta"), ag); tx.AssignCohort([]byte("key"), ag)`。
- adopt（收编已有独立 bucket）、delete member、rollback 撤销未提交 adopt 均已处理；`Tx.Size` / `WriteTo` 也走 cohort 解析 → 快照对 cohort 成员联合一致。

**调试中修的 bug**：普通 `Put` 到 cohort 成员不会标记 cohort dirty（只有 `AssignCohort`/`DeleteBucket` 会）→ 提交时 cohort 快照没重建 → 写丢失。修：commit 时对每个 touched 的 cohort 成员 `markCohortDirty`。

### 10.5 验证
新增 `snapshot_test.go`（有序 Cursor、`WriteTo→Restore` 往返 CRC 一致、`Open` 自动恢复、`Tx.Size` 增长）、`cohort_test.go`（BasicCRUD、**JointAtomicVisibility**——并发写者推进 `meta.v`/`key.v` 同值 + 读者断言始终相等 0 mismatch、AdoptExisting、DeleteMember）。`go test ./...` 全绿、`-race` 无竞争、`go vet` 干净。

### 10.6 etcd 就绪度小结
| 需求 | 状态 |
|------|------|
| 顶层扁平 bucket（etcd 只用顶层，从不嵌套） | ✅ 契合 |
| `Begin/Commit/Bucket/Put/Delete/Cursor/ForEach/FillPercent/Stats/Options` | ✅ |
| 有序 `tx.Cursor()`（Hash/defrag 确定性） | ✅ |
| `WriteTo`/快照流（raft snapshot 传输） | ✅ BMSP |
| `Tx.Size()` | ✅ |
| `meta`+`key` 联合原子（consistent_index） | ✅ cohort |
| 持久化/启动恢复 | 🟠 仅 BMSP 快照按需落盘 + 恢复；提交间易失（非持久化 etcd 定位） |
| defrag | 🟠 内存无碎片，可 no-op（v3.6 仅 online） |

### 10.7 本节相关文件
新增：`snapshot.go`（BMSP 序列化/反序列化）、`cohort.go`（Cohort/AssignCohort/pinCohort）；改：`cursor.go`（directory 模式）、`bucket_state.go`（cohort 类型 + `publishedStateOf`）、`tx.go`（`Cursor`/`Size`/cohort 提交/回滚）、`db.go`（`NewCohort`/`Open` 自动恢复）、`bucket.go`（base 经 cohort 解析）。测试：`snapshot_test.go`、`cohort_test.go`。
