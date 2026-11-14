<script setup>
import { onMounted, ref } from 'vue'
import { getJSON } from '../api'
const rows = ref([])
const leaseByStream = ref({})
const err = ref('')

const STATE = {
  free: '空闲',
  active: '持有中',
  expired: '已过期',
  released: '已释放',
  preempted: '已抢占',
  reclaimed: '已回收',
}

onMounted(async () => {
  try {
    rows.value = await getJSON('/api/streams')
    const body = await getJSON('/api/leases')
    leaseByStream.value = Object.fromEntries(body.leases.map((l) => [l.stream_id, l]))
  } catch (e) {
    err.value = String(e.message || e)
  }
})
</script>
<template>
  <div class="page">
    <h1>投影流</h1>
    <p class="muted">每条流绑定 CDC 夹具、实体键名与载荷字段，并各持一份带过期时间的投影租约。</p>
    <p v-if="err" class="tag danger">{{ err }}</p>
    <div class="panel">
      <table>
        <thead>
          <tr>
            <th>ID</th><th>编码</th><th>标题</th><th>业务域</th>
            <th>实体键</th><th>载荷字段</th><th>夹具</th><th>水位 seq</th><th>租约</th><th>启用</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="s in rows" :key="s.id">
            <td>{{ s.id }}</td>
            <td class="mono">{{ s.code }}</td>
            <td>{{ s.title }}</td>
            <td>{{ s.domain }}</td>
            <td class="mono">{{ s.entity_key_name }}</td>
            <td class="mono">{{ s.payload_fields.join(', ') }}</td>
            <td class="mono">{{ s.fixture }}</td>
            <td class="mono">{{ s.watermark_seq }}</td>
            <td>
              <span
                class="tag"
                :class="{
                  ok: ['released', 'reclaimed', 'free'].includes(leaseByStream[s.id]?.state),
                  warn: leaseByStream[s.id]?.state === 'active',
                  danger: ['expired', 'preempted'].includes(leaseByStream[s.id]?.state),
                }"
              >
                {{ STATE[leaseByStream[s.id]?.state] || '空闲' }}
              </span>
            </td>
            <td><span class="tag" :class="s.enabled ? 'ok' : 'danger'">{{ s.enabled ? '是' : '否' }}</span></td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
