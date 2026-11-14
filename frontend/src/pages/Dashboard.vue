<script setup>
import { onMounted, ref } from 'vue'
import { getJSON } from '../api'
const stats = ref(null)
const leases = ref([])
const err = ref('')

const STATE = {
  free: { label: '空闲', cls: '' },
  active: { label: '持有中', cls: 'warn' },
  expired: { label: '已过期', cls: 'danger' },
  released: { label: '已释放', cls: 'ok' },
  preempted: { label: '已抢占', cls: 'danger' },
  reclaimed: { label: '已回收', cls: 'ok' },
}

function leaseState(s) {
  return STATE[s] || { label: s, cls: '' }
}

onMounted(async () => {
  try {
    stats.value = await getJSON('/api/dashboard')
    const body = await getJSON('/api/leases')
    leases.value = body.leases
  } catch (e) {
    err.value = String(e.message || e)
  }
})
</script>
<template>
  <div class="page">
    <h1>总览</h1>
    <p class="muted">
      变更日志投影：源 CDC 事件按序落到当前投影，批次计数与快照互证。开跑受流租约约束，过期按策略抢占或拒绝。
    </p>
    <p v-if="err" class="tag danger">{{ err }}</p>
    <div class="panel" v-if="stats">
      <div class="stat-grid">
        <div class="stat"><div class="stat-num">{{ stats.stream_count }}</div><div class="stat-label">投影流</div></div>
        <div class="stat"><div class="stat-num">{{ stats.batch_count }}</div><div class="stat-label">应用批次</div></div>
        <div class="stat"><div class="stat-num">{{ stats.projection_alive }}</div><div class="stat-label">存活投影</div></div>
        <div class="stat"><div class="stat-num">{{ stats.projection_deleted }}</div><div class="stat-label">已删投影</div></div>
        <div class="stat"><div class="stat-num">{{ stats.lease_held_streams }}</div><div class="stat-label">租约持有中</div></div>
        <div class="stat"><div class="stat-num">{{ stats.lease_expired_streams }}</div><div class="stat-label">租约已过期</div></div>
        <div class="stat"><div class="stat-num">{{ stats.preempted_batches }}</div><div class="stat-label">被抢占批次</div></div>
        <div class="stat"><div class="stat-num">{{ stats.fenced_batches }}</div><div class="stat-label">栅栏拦截</div></div>
      </div>
      <p style="margin-top:1rem">
        默认应用序 <span class="tag">{{ stats.apply_order }}</span>（按事件 seq 升序）
        · 过期策略
        <span class="tag" :class="stats.lease_preempt_policy === 'allow_preempt' ? 'warn' : ''">
          {{ stats.lease_preempt_policy === 'allow_preempt' ? '允许抢占' : '禁止抢占' }}
        </span>
        · 默认租约 TTL <span class="tag">{{ stats.lease_default_ttl }}s</span>
      </p>
      <p><router-link to="/run">去投影台跑一趟 →</router-link> · <router-link to="/leases">去租约台演示并发与抢占 →</router-link></p>
    </div>

    <div class="panel" v-if="leases.length">
      <h3 style="margin-top:0">各流租约</h3>
      <table>
        <thead>
          <tr><th>投影流</th><th>状态</th><th>持有者</th><th>代号</th><th>剩余秒</th><th>水位 seq</th><th>拒绝</th><th>抢占</th><th>回收</th></tr>
        </thead>
        <tbody>
          <tr v-for="l in leases" :key="l.stream_id">
            <td>{{ l.stream_title }}</td>
            <td><span class="tag" :class="leaseState(l.state).cls">{{ leaseState(l.state).label }}</span></td>
            <td class="mono">{{ l.owner || '—' }}</td>
            <td class="mono">{{ l.generation ?? '—' }}</td>
            <td class="mono">{{ l.remaining_seconds ?? '—' }}</td>
            <td class="mono">{{ l.watermark_seq }}</td>
            <td class="mono">{{ l.counters.reject_count }}</td>
            <td class="mono">{{ l.counters.preempt_count }}</td>
            <td class="mono">{{ l.counters.reclaim_count }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
