<script setup>
import { onMounted, ref } from 'vue'
import { getJSON } from '../api'
const rows = ref([])
const err = ref('')

const STATUS = {
  pending: { label: '进行中（占租约）', cls: 'warn' },
  completed: { label: '已完成', cls: 'ok' },
  preempted: { label: '已抢占作废', cls: 'danger' },
  failed: { label: '已失败', cls: 'danger' },
  fenced: { label: '被栅栏拦下', cls: 'danger' },
}

function status(s) {
  return STATUS[s] || { label: s, cls: '' }
}

onMounted(async () => {
  try { rows.value = await getJSON('/api/batches?limit=100') } catch (e) { err.value = String(e.message || e) }
})
</script>
<template>
  <div class="page">
    <h1>应用批次</h1>
    <p class="muted">
      每次投影落一批次：源事件数、写入/删除、存活/已删与水位 seq。被抢占、失败或被栅栏拦下的批次
      不再允许提交投影或水位，并保留作废原因。
    </p>
    <p v-if="err" class="tag danger">{{ err }}</p>
    <div class="panel">
      <table>
        <thead>
          <tr>
            <th>批次</th><th>投影流</th><th>状态</th><th>租约代号</th>
            <th>源事件</th><th>写入</th><th>删除</th><th>跳过</th>
            <th>存活</th><th>已删</th><th>max seq</th><th>作废原因</th><th>时间</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="b in rows" :key="b.id">
            <td class="mono">{{ b.batch_no }}</td>
            <td>{{ b.stream_title }}</td>
            <td><span class="tag" :class="status(b.status).cls">{{ status(b.status).label }}</span></td>
            <td class="mono">{{ b.lease_generation ?? '—' }}</td>
            <td>{{ b.source_events }}</td>
            <td>{{ b.applied_upsert }}</td>
            <td>{{ b.applied_delete }}</td>
            <td>{{ b.skipped_duplicate }}</td>
            <td>{{ b.projected_alive }}</td>
            <td>{{ b.projected_deleted }}</td>
            <td class="mono">{{ b.max_seq }}</td>
            <td class="muted">{{ b.fence_reason || '—' }}</td>
            <td class="mono">{{ b.created_at }}</td>
          </tr>
          <tr v-if="!rows.length"><td colspan="13" class="muted">尚无批次。</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
