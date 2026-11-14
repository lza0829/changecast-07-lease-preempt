<script setup>
import { onMounted, ref } from 'vue'
import { getJSON, postJSON } from '../api'

const streams = ref([])
const batches = ref([])
const streamId = ref(null)
const note = ref('')
const ttl = ref(30)
const last = ref(null)
const busy = ref(false)
const err = ref('')

const STATUS = {
  pending: { label: '进行中（占租约）', cls: 'warn' },
  completed: { label: '已完成', cls: 'ok' },
  preempted: { label: '已抢占作废', cls: 'danger' },
  failed: { label: '已失败', cls: 'danger' },
  fenced: { label: '被栅栏拦下', cls: 'danger' },
}

function statusLabel(s) {
  return STATUS[s]?.label || s
}

function statusClass(s) {
  return STATUS[s]?.cls || ''
}

async function refresh() {
  streams.value = await getJSON('/api/streams')
  batches.value = await getJSON('/api/batches?limit=20')
  if (streamId.value == null && streams.value.length) streamId.value = streams.value[0].id
}

async function run() {
  err.value = ''
  busy.value = true
  try {
    last.value = await postJSON('/api/project', {
      stream_id: Number(streamId.value),
      note: note.value || null,
      ttl_seconds: Number(ttl.value) || null,
    })
    await refresh()
  } catch (e) {
    err.value = `${e.code ? '[' + e.code + '] ' : ''}${e.message}`
  } finally {
    busy.value = false
  }
}

onMounted(async () => {
  try { await refresh() } catch (e) { err.value = String(e.message || e) }
})
</script>
<template>
  <div class="page">
    <h1>投影台</h1>
    <p class="muted">
      选一条投影流，开跑须领取带过期时间的流租约；提交前校验令牌，过期或被抢占的旧批会被栅栏挡住。
      需要演示并发、心跳与抢占请到 <router-link to="/leases">租约台</router-link>。
    </p>
    <p v-if="err" class="tag danger" style="display:block">{{ err }}</p>

    <div class="panel">
      <h3 style="margin-top:0">发起投影</h3>
      <div style="display:flex;flex-wrap:wrap;gap:0.75rem;align-items:flex-end">
        <label>
          投影流
          <select v-model.number="streamId" style="display:block;min-width:18rem;margin-top:0.25rem">
            <option v-for="s in streams" :key="s.id" :value="s.id">{{ s.title }}</option>
          </select>
        </label>
        <label>
          备注（可选）
          <input v-model="note" style="display:block;width:16rem;margin-top:0.25rem" placeholder="本趟说明" />
        </label>
        <label>
          租约 TTL（秒）
          <input v-model.number="ttl" type="number" min="2" max="3600" style="display:block;width:7rem;margin-top:0.25rem" />
        </label>
        <button :disabled="busy || !streamId" @click="run">{{ busy ? '投影中…' : '应用变更流' }}</button>
      </div>
    </div>

    <div class="panel" v-if="last">
      <h3 style="margin-top:0">最近一趟</h3>
      <p>
        批次 <span class="mono">{{ last.batch_no }}</span> · {{ last.stream_title }} ·
        <span class="tag ok">{{ last.status }}</span>
        <span class="tag mono">租约 g{{ last.lease_generation }}</span>
      </p>
      <p class="mono muted" style="margin-top:-0.4rem">token {{ last.lease_token }} · 提交后租约即释放</p>
      <div class="stat-grid">
        <div class="stat"><div class="stat-num">{{ last.source_events }}</div><div class="stat-label">源事件</div></div>
        <div class="stat"><div class="stat-num">{{ last.applied_upsert }}</div><div class="stat-label">写入</div></div>
        <div class="stat"><div class="stat-num">{{ last.applied_delete }}</div><div class="stat-label">删除</div></div>
        <div class="stat"><div class="stat-num">{{ last.skipped_duplicate }}</div><div class="stat-label">跳过重复</div></div>
        <div class="stat"><div class="stat-num">{{ last.projected_alive }}</div><div class="stat-label">存活</div></div>
        <div class="stat"><div class="stat-num">{{ last.projected_deleted }}</div><div class="stat-label">已删</div></div>
      </div>
      <p style="margin-top:0.75rem">
        <router-link :to="{ path: '/projections', query: { stream_id: last.stream_id } }">查看投影快照 →</router-link>
        ·
        <router-link :to="{ path: '/events', query: { batch_id: last.id } }">事件留痕 →</router-link>
      </p>
    </div>

    <div class="panel">
      <h3 style="margin-top:0">历史批次</h3>
      <table>
        <thead>
          <tr>
            <th>批次</th><th>投影流</th><th>状态</th><th>租约代号</th>
            <th>源事件</th><th>写入</th><th>删除</th><th>存活</th><th>已删</th><th>时间</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="b in batches" :key="b.id">
            <td class="mono">{{ b.batch_no }}</td>
            <td>{{ b.stream_title }}</td>
            <td><span class="tag" :class="statusClass(b.status)">{{ statusLabel(b.status) }}</span></td>
            <td class="mono">{{ b.lease_generation ?? '—' }}</td>
            <td>{{ b.source_events }}</td>
            <td>{{ b.applied_upsert }}</td>
            <td>{{ b.applied_delete }}</td>
            <td>{{ b.projected_alive }}</td>
            <td>{{ b.projected_deleted }}</td>
            <td class="mono">{{ b.created_at }}</td>
          </tr>
          <tr v-if="!batches.length"><td colspan="10" class="muted">还没有批次，先跑一趟。</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
