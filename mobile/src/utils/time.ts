const BEIJING_OFFSET_MS = 8 * 60 * 60 * 1000

function pad2(value: number): string {
  return String(value).padStart(2, '0')
}

export function formatBeijingDateTime(raw: string): string {
  const source = String(raw || '').trim()
  if (!source) return ''

  const normalized = source.replace('T', ' ')
  const match = normalized.match(/^(\d{4})-(\d{2})-(\d{2})[ ](\d{2}):(\d{2})(?::(\d{2}))?/)
  if (!match) return normalized.slice(0, 16)

  const [, year, month, day, hour, minute, second = '00'] = match
  const hasTimezone = /(?:Z|[+-]\d{2}:?\d{2})$/.test(source)
  const utcMs = hasTimezone
    ? Date.parse(source)
    : Date.UTC(
        Number(year),
        Number(month) - 1,
        Number(day),
        Number(hour),
        Number(minute),
        Number(second),
      )
  if (!Number.isFinite(utcMs)) return normalized.slice(0, 16)

  const beijing = new Date(utcMs + BEIJING_OFFSET_MS)
  return [
    beijing.getUTCFullYear(),
    '-',
    pad2(beijing.getUTCMonth() + 1),
    '-',
    pad2(beijing.getUTCDate()),
    ' ',
    pad2(beijing.getUTCHours()),
    ':',
    pad2(beijing.getUTCMinutes()),
  ].join('')
}
