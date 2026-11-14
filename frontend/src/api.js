export async function getJSON(url) {
  const r = await fetch(url)
  if (!r.ok) throw await toError(r)
  return r.json()
}

export async function postJSON(url, body = {}) {
  return sendJSON('POST', url, body)
}

export async function putJSON(url, body = {}) {
  return sendJSON('PUT', url, body)
}

async function sendJSON(method, url, body) {
  const r = await fetch(url, {
    method,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!r.ok) throw await toError(r)
  return r.json()
}

async function toError(r) {
  const text = await r.text()
  let detail = text
  try {
    detail = JSON.parse(text).detail ?? text
  } catch {
    /* keep raw text */
  }
  if (detail && typeof detail === 'object') {
    const err = new Error(detail.message || detail.code || text)
    err.code = detail.code
    err.detail = detail
    err.status = r.status
    return err
  }
  const err = new Error(String(detail))
  err.status = r.status
  return err
}
