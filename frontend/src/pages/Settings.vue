<script setup>
import { computed, onMounted, ref } from 'vue'
import { getJSON, putJSON } from '../api'
const settings = ref({})
const ttl = ref(120)
const err = ref('')
const ok = ref('')

const policy = computed(() => settings.value.lease_preempt_policy || 'refuse_preempt')

async function load() {
  settings.value = await getJSON('/api/settings')
  ttl.value = Number(settings.value.lease_default_ttl || 120)
}

async function setPolicy(next) {
  err.value = ''
  ok.value = ''
  try {
    const body = await putJSON('/api/leases/policy', { policy: next, default_ttl: Number(ttl.value) })
    ok.value = `已保存：${body.policy}，默认 TTL ${body.default_ttl_seconds}s`
    await load()
  } catch (e) {
    err.value = String(e.message || e)
  }
}

onMounted(async () => {
  try { await load() } catch (e) { err.value = String(e.message || e) }
})
</script>
<template>
  <div class="page">
    <h1>设置</h1>
    <p class="muted">应用序与重复策略只读；租约过期后的互斥策略可切换，保存后对后续开跑立即生效。</p>
    <p v-if="err" class="tag danger">{{ err }}</p>
    <p v-if="ok" class="tag ok">{{ ok }}</p>

    <div class="panel">
      <h3 style="margin-top:0">租约过期策略</h3>
      <div style="display:flex;flex-wrap:wrap;gap:0.6rem;align-items:center">
        <button :class="{ ghost: policy !== 'allow_preempt' }" @click="setPolicy('allow_preempt')">允许抢占</button>
        <button :class="{ ghost: policy !== 'refuse_preempt' }" @click="setPolicy('refuse_preempt')">禁止抢占</button>
        <label style="margin-left:0.6rem">
          默认 TTL（秒）
          <input v-model.number="ttl" type="number" min="2" max="3600" style="width:6rem;margin-left:0.35rem" />
        </label>
        <button @click="setPolicy(policy)">保存 TTL</button>
      </div>
      <p class="muted" style="margin-bottom:0">
        {{
          policy === 'allow_preempt'
            ? '过期后新开跑作废旧批并记抢占原因；旧批不得再提交投影/水位。'
            : '过期后仍拒绝新开跑，直到显式释放或回收接口把旧批标失败并释锁。'
        }}
      </p>
    </div>

    <div class="panel">
      <h3 style="margin-top:0">策略键值</h3>
      <table>
        <thead><tr><th>键</th><th>值</th></tr></thead>
        <tbody>
          <tr v-for="(v, k) in settings" :key="k">
            <td class="mono">{{ k }}</td>
            <td><span class="tag">{{ v }}</span></td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<style scoped>
button.ghost { background: #e4e9ef; color: var(--ink); border: 1px solid var(--line); }
</style>
