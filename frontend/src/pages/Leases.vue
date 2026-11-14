<script setup>
// 租约台：演示同流并发开跑只放行第一趟、心跳续租、过期后按策略抢占/拒绝、旧批提交被栅栏挡住。
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'
import { getJSON, postJSON, putJSON } from '../api'

const route = useRoute()
const streams = ref([])
const overview = ref({ policy: 'refuse_preempt', default_ttl_seconds: 120, leases: [] })
const events = ref([])
const audit = ref(null)
const streamId = ref(null)
const ttl = ref(20)
const owner = ref('worker-A')
const token = ref('')
const log = ref([])
const err = ref('')
const crashed = ref(null) // { batch_id, batch_no, lease_token, stream_id }
const tick = ref(0)

const POLICY_LABEL = {
  allow_preempt: '允许抢占',
  refuse_preempt: '禁止抢占',
}
const KIND_LABEL = {
  acquired: '领取',
  rejected: '拒绝',
  heartbeat: '续租',
  preempt: '抢占',
  reclaim: '回收',
  released: '释放',
  completed: '提交完成',
  fenced_commit: '栅栏拦截',
}
const STATE_LABEL = {
  free: '空闲',
  active: '持有中',
  expired: '已过期（未回收）',
  released: '已释放',
  preempted: '已抢占',
  reclaimed: '已回收',
}

const current = computed(
  () => overview.value.leases.find((l) => l.stream_id === Number(streamId.value)) || null
)
const policy = computed(() => overview.value.policy)

function say(kind, text) {
  log.value.unshift({
    at: new Date().toLocaleTimeString(),
    kind,
    text,
  })
  log.value = log.value.slice(0, 30)
}

async function refresh() {
  const [s, o] = await Promise.all([getJSON('/api/streams'), getJSON('/api/leases')])
  streams.value = s
  overview.value = o
  if (streamId.value == null && s.length) streamId.value = s[0].id
  events.value = await getJSON(
    '/api/leases/events' + (streamId.value ? `?stream_id=${streamId.value}&limit=60` : '?limit=60')
  )
  audit.value = await getJSON('/api/leases/audit')
}

async function guard(fn) {
  err.value = ''
  try {
    await fn()
  } catch (e) {
    err.value = `${e.code ? '[' + e.code + '] ' : ''}${e.message}`
    say('err', err.value)
  }
  try {
    await refresh()
  } catch (e) {
    /* keep last snapshot */
  }
}

function runSecondStream() {
  return guard(async () => {
    const body = await postJSON('/api/project', { stream_id: Number(streamId.value), note: '第二趟开跑' })
    say('ok', `第二趟竟然成功：${body.batch_no}`)
  })
}

function runFirstStream() {
  return guard(async () => {
    const body = await postJSON('/api/project', { stream_id: Number(streamId.value), note: '第一趟开跑' })
    token.value = body.lease_token || ''
    say('ok', `${body.batch_no} 提交完成，水位 seq=${body.max_seq}，租约已释放`)
    crashed.value = null
  })
}

function holdLease() {
  return guard(async () => {
    const body = await postJSON('/api/leases/acquire', {
      stream_id: Number(streamId.value),
      owner: owner.value || 'worker-A',
      ttl_seconds: Number(ttl.value),
    })
    token.value = body.lease_token
    say('ok', `${owner.value} 领取租约 ${body.ttl_seconds}s，到期于 ${body.expires_at}`)
  })
}

function heartbeat() {
  return guard(async () => {
    const body = await postJSON('/api/leases/heartbeat', {
      stream_id: Number(streamId.value),
      lease_token: token.value,
      ttl_seconds: Number(ttl.value),
    })
    say('ok', `心跳续租至 ${body.expires_at}（剩余 ${body.remaining_seconds}s）`)
  })
}

function release() {
  return guard(async () => {
    const body = await postJSON('/api/leases/release', {
      stream_id: Number(streamId.value),
      lease_token: token.value || null,
      reason: '显式释放',
    })
    say('ok', `租约已释放，计数 release=${body.counters.release_count}`)
    crashed.value = null
  })
}

function reclaim() {
  return guard(async () => {
    const body = await postJSON('/api/leases/reclaim', {
      stream_id: Number(streamId.value),
      reason: '后台回收过期租约',
    })
    say('ok', `已回收过期租约，标记旧批失败；reclaim=${body.counters.reclaim_count}`)
  })
}

// 模拟进程崩溃：领取租约并留下 pending 批次，但不提交。
function crashRun() {
  return guard(async () => {
    const body = await postJSON('/api/runs/start', {
      stream_id: Number(streamId.value),
      owner: owner.value || 'worker-A',
      ttl_seconds: Number(ttl.value),
      note: '崩溃前开跑',
    })
    crashed.value = {
      stream_id: Number(streamId.value),
      batch_id: body.batch.id,
      batch_no: body.batch.batch_no,
      lease_token: body.lease_token,
    }
    token.value = body.lease_token
    say('warn', `${body.batch.batch_no} 已暂存并占租约，进程崩溃未提交`)
  })
}

// 旧批（崩溃进程）事后恢复并尝试提交：必须被栅栏挡住。
function lateCommit() {
  return guard(async () => {
    if (!crashed.value) throw new Error('先点「模拟崩溃开跑」，才有旧批可提交')
    const body = await postJSON('/api/runs/commit', {
      batch_id: crashed.value.batch_id,
      lease_token: crashed.value.lease_token,
    })
    say('bad', `旧批竟然提交成功：${body.batch_no}`)
  })
}

function switchPolicy(next) {
  return guard(async () => {
    const body = await putJSON('/api/leases/policy', { policy: next, default_ttl: Number(ttl.value) })
    say('ok', `策略切换为 ${POLICY_LABEL[body.policy]}（默认 TTL ${body.default_ttl_seconds}s）`)
  })
}

let timer = null
onMounted(async () => {
  try {
    if (route.query.stream_id) streamId.value = Number(route.query.stream_id)
    await refresh()
    timer = setInterval(() => {
      tick.value++
      refresh().catch(() => {})
    }, 2000)
  } catch (e) {
    err.value = String(e.message || e)
  }
})
onBeforeUnmount(() => timer && clearInterval(timer))

function kindClass(kind) {
  if (kind === 'acquired' || kind === 'completed' || kind === 'released') return 'ok'
  if (kind === 'rejected' || kind === 'fenced_commit') return 'danger'
  if (kind === 'preempt' || kind === 'reclaim') return 'warn'
  return ''
}
</script>

<template>
  <div class="page">
    <h1>租约台</h1>
    <p class="muted">
      投影流开跑必须领取带过期时间的租约；租约内同流其他开跑一律拒绝。进程崩溃后批次不再永久占位：
      租约过期后按策略<b>抢占</b>或<b>回收</b>，旧批的迟到提交一律被栅栏挡住。
    </p>

    <p v-if="err" class="tag danger" style="display:block">{{ err }}</p>

    <div class="panel">
      <h3 style="margin-top:0">过期策略</h3>
      <div class="policy-row">
        <button
          class="policy-btn"
          :class="{ active: policy === 'allow_preempt' }"
          @click="switchPolicy('allow_preempt')"
        >
          允许抢占
        </button>
        <button
          class="policy-btn"
          :class="{ active: policy === 'refuse_preempt' }"
          @click="switchPolicy('refuse_preempt')"
        >
          禁止抢占
        </button>
        <span class="muted">
          {{
            policy === 'allow_preempt'
              ? '过期后新开跑作废旧批（旧批不得再提交投影/水位），并记录抢占原因'
              : '过期后仍拒绝新开跑，直到显式释放或回收接口把旧批标失败并释锁'
          }}
        </span>
      </div>
    </div>

    <div class="panel">
      <h3 style="margin-top:0">操作台</h3>
      <div class="ctl-row">
        <label>
          投影流
          <select v-model.number="streamId" style="display:block;min-width:15rem;margin-top:0.25rem">
            <option v-for="s in streams" :key="s.id" :value="s.id">{{ s.title }}</option>
          </select>
        </label>
        <label>
          租约持有者
          <input v-model="owner" style="display:block;width:11rem;margin-top:0.25rem" />
        </label>
        <label>
          租约 TTL（秒）
          <input v-model.number="ttl" type="number" min="2" max="3600" style="display:block;width:6rem;margin-top:0.25rem" />
        </label>
        <label>
          当前令牌
          <input v-model="token" class="mono" style="display:block;width:16rem;margin-top:0.25rem" placeholder="领取/心跳接口返回" />
        </label>
      </div>

      <div class="btn-row">
        <button @click="holdLease">领取租约</button>
        <button @click="heartbeat">心跳续租</button>
        <button class="ghost" @click="release">显式释放</button>
        <button class="ghost" @click="reclaim">回收过期租约</button>
      </div>
      <div class="btn-row">
        <button @click="runFirstStream">开跑（带租约整趟提交）</button>
        <button class="ghost" @click="runSecondStream">同流再开跑（应被拒）</button>
        <button class="warn-btn" @click="crashRun">模拟崩溃开跑（占租约不提交）</button>
        <button class="warn-btn" :disabled="!crashed" @click="lateCommit">旧批迟到提交（应被栅栏挡）</button>
      </div>
      <p v-if="crashed" class="muted mono" style="margin-bottom:0">
        崩溃批次 {{ crashed.batch_no }} · batch_id={{ crashed.batch_id }} · token={{ crashed.lease_token }}
      </p>
    </div>

    <div class="panel" v-if="current">
      <h3 style="margin-top:0">当前租约 · {{ current.stream_title }}</h3>
      <div class="stat-grid">
        <div class="stat">
          <div class="stat-num state-num">{{ STATE_LABEL[current.state] || current.state }}</div>
          <div class="stat-label">租约状态</div>
        </div>
        <div class="stat">
          <div class="stat-num">{{ current.remaining_seconds ?? '—' }}</div>
          <div class="stat-label">剩余秒数</div>
        </div>
        <div class="stat">
          <div class="stat-num">{{ current.generation ?? '—' }}</div>
          <div class="stat-label">租约代号</div>
        </div>
        <div class="stat">
          <div class="stat-num">{{ current.counters.reject_count }}</div>
          <div class="stat-label">拒绝次数</div>
        </div>
        <div class="stat">
          <div class="stat-num">{{ current.counters.preempt_count }}</div>
          <div class="stat-label">抢占次数</div>
        </div>
        <div class="stat">
          <div class="stat-num">{{ current.counters.reclaim_count }}</div>
          <div class="stat-label">回收次数</div>
        </div>
        <div class="stat">
          <div class="stat-num">{{ current.counters.heartbeat_count }}</div>
          <div class="stat-label">心跳次数</div>
        </div>
        <div class="stat">
          <div class="stat-num">{{ current.counters.fence_count }}</div>
          <div class="stat-label">栅栏拦截</div>
        </div>
      </div>
      <table style="margin-top:0.9rem">
        <tbody>
          <tr><th>持有者</th><td class="mono">{{ current.owner || '—' }}</td><th>占用批次</th><td class="mono">{{ current.batch_no || '—' }}</td></tr>
          <tr><th>租约令牌</th><td class="mono">{{ current.lease_token || '—' }}</td><th>到期时间</th><td class="mono">{{ current.expires_at || '—' }}</td></tr>
          <tr><th>领取时间</th><td class="mono">{{ current.acquired_at || '—' }}</td><th>最近心跳</th><td class="mono">{{ current.last_heartbeat_at || '—' }}</td></tr>
          <tr><th>水位 seq</th><td class="mono">{{ current.watermark_seq }}</td><th>结束原因</th><td>{{ current.end_reason || '—' }}</td></tr>
        </tbody>
      </table>
    </div>

    <div class="panel" v-if="audit">
      <h3 style="margin-top:0">
        投影与水位互证
        <span class="tag" :class="audit.ok ? 'ok' : 'danger'">{{ audit.ok ? '全部通过' : '存在不一致' }}</span>
      </h3>
      <table>
        <thead><tr><th>投影流</th><th>校验项</th><th>结果</th><th>说明</th></tr></thead>
        <tbody>
          <tr v-for="(c, i) in audit.checks" :key="i">
            <td class="mono">{{ c.stream_code }}</td>
            <td class="mono">{{ c.check }}</td>
            <td><span class="tag" :class="c.ok ? 'ok' : 'danger'">{{ c.ok ? '一致' : '不一致' }}</span></td>
            <td class="muted">{{ c.detail }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div class="panel">
      <h3 style="margin-top:0">租约账本</h3>
      <table>
        <thead><tr><th>#</th><th>种类</th><th>批次</th><th>令牌</th><th>代号</th><th>原因</th><th>说明</th><th>时间</th></tr></thead>
        <tbody>
          <tr v-for="e in events" :key="e.id">
            <td>{{ e.id }}</td>
            <td><span class="tag" :class="kindClass(e.kind)">{{ KIND_LABEL[e.kind] || e.kind }}</span></td>
            <td class="mono">{{ e.batch_no || '—' }}</td>
            <td class="mono">{{ e.lease_token || '—' }}</td>
            <td class="mono">{{ e.generation ?? '—' }}</td>
            <td class="mono">{{ e.reason || '—' }}</td>
            <td class="muted">{{ e.detail || '—' }}</td>
            <td class="mono">{{ e.created_at }}</td>
          </tr>
          <tr v-if="!events.length"><td colspan="8" class="muted">尚无租约事件。</td></tr>
        </tbody>
      </table>
    </div>

    <div class="panel">
      <h3 style="margin-top:0">本次操作记录</h3>
      <ul class="log-list">
        <li v-for="(l, i) in log" :key="i" :class="'log-' + l.kind">
          <span class="mono">{{ l.at }}</span> {{ l.text }}
        </li>
        <li v-if="!log.length" class="muted">操作后这里会出现记录。</li>
      </ul>
    </div>
  </div>
</template>

<style scoped>
.policy-row { display: flex; flex-wrap: wrap; gap: 0.6rem; align-items: center; }
.policy-btn { background: #dde3ea; color: var(--ink); }
.policy-btn.active { background: var(--accent); color: #fff; }
.ctl-row { display: flex; flex-wrap: wrap; gap: 0.9rem; align-items: flex-end; }
.btn-row { display: flex; flex-wrap: wrap; gap: 0.5rem; margin-top: 0.85rem; }
button.ghost { background: #e4e9ef; color: var(--ink); border: 1px solid var(--line); }
button.warn-btn { background: var(--warn); }
.state-num { font-size: 1.1rem; }
.log-list { list-style: none; padding: 0; margin: 0; font-size: 0.9rem; }
.log-list li { padding: 0.2rem 0; border-bottom: 1px dotted var(--line); }
.log-err, .log-bad { color: var(--danger); }
.log-warn { color: var(--warn); }
</style>
