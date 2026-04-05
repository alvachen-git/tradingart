export interface MobileAiFormatted {
  fullText: string
  previewText: string
  hasMore: boolean
}

const PREVIEW_LIMIT = 260
const PREVIEW_MIN = 140
const SUMMARY_KEYS = [
  '最终决策',
  '核心结论',
  '风险提示',
  '结论',
  '策略建议',
  '执行建议',
]
const PREVIEW_EXTRA_KEYS = ['建议', '风险', '结论', '策略', '止损']

function normalizeNewline(text: string): string {
  return text.replace(/\r\n?/g, '\n')
}

function stripInlineMarkdown(text: string): string {
  return text
    .replace(/\[(.*?)\]\((.*?)\)/g, '$1')
    .replace(/\*\*(.*?)\*\*/g, '$1')
    .replace(/__(.*?)__/g, '$1')
    .replace(/`([^`]+)`/g, '$1')
    .replace(/~~(.*?)~~/g, '$1')
    .replace(/\*([^*\n]+)\*/g, '$1')
    .replace(/_([^_\n]+)_/g, '$1')
}

function splitTableCells(line: string): string[] {
  const body = line.trim().replace(/^\|/, '').replace(/\|$/, '')
  return body.split('|').map((cell) => stripInlineMarkdown(cell).trim())
}

function isTableSeparatorRow(cells: string[]): boolean {
  if (!cells.length) return false
  return cells.every((cell) => /^:?-{2,}:?$/.test(cell.replace(/\s+/g, '')))
}

function isLikelyTableLine(line: string): boolean {
  const s = line.trim()
  if (!s || !s.includes('|')) return false
  const cells = splitTableCells(s)
  if (cells.length < 2) return false
  if (isTableSeparatorRow(cells)) return true
  return cells.some((cell) => !!cell)
}

function convertTableBlock(lines: string[]): string[] {
  const rows = lines
    .map((line) => splitTableCells(line))
    .filter((cells) => cells.some((cell) => cell.length > 0))

  if (!rows.length) return []

  const headers = rows[0].map((h, idx) => h || `列${idx + 1}`)
  const hasSeparator = rows.length > 1 && isTableSeparatorRow(rows[1])
  let dataRows = rows.slice(hasSeparator ? 2 : 1)
  if (!dataRows.length) dataRows = rows.slice(1)
  if (!dataRows.length) return [`• ${headers.join('；')}`]

  return dataRows.map((row) => {
    const pairs: string[] = []
    const maxLen = Math.max(headers.length, row.length)
    for (let i = 0; i < maxLen; i += 1) {
      const key = headers[i] || `列${i + 1}`
      const val = (row[i] || '-').trim() || '-'
      pairs.push(`${key}：${val}`)
    }
    return `• ${pairs.join('；')}`
  })
}

function normalizeMarkdownToMobile(raw: string): string {
  const text = normalizeNewline(String(raw || ''))
  const lines = text.split('\n')
  const out: string[] = []

  for (let i = 0; i < lines.length;) {
    const line = lines[i]

    if (isLikelyTableLine(line)) {
      const block: string[] = []
      while (i < lines.length && isLikelyTableLine(lines[i])) {
        block.push(lines[i])
        i += 1
      }
      out.push(...convertTableBlock(block))
      continue
    }

    i += 1
    let s = line
      .replace(/^\s{0,3}#{1,6}\s*/, '')
      .replace(/^\s*>\s?/, '')
      .replace(/^\s{0,6}[-*+]\s+/, '• ')
      .replace(/^\s*\d+[.)]\s+/, '• ')

    s = stripInlineMarkdown(s)
    s = s.replace(/[ \t]{2,}/g, ' ').trim()
    if (/^[-=]{3,}$/.test(s)) s = ''
    out.push(s)
  }

  const compact: string[] = []
  for (const line of out) {
    if (!line) {
      if (!compact.length || compact[compact.length - 1] === '') continue
      compact.push('')
      continue
    }
    compact.push(line)
  }

  while (compact.length && compact[0] === '') compact.shift()
  while (compact.length && compact[compact.length - 1] === '') compact.pop()
  return compact.join('\n')
}

function hasSummaryKeyword(text: string): boolean {
  return SUMMARY_KEYS.some((k) => text.includes(k))
}

function extractPreviewBody(body: string): string {
  const text = String(body || '').trim()
  if (!text) return ''
  if (text.length <= PREVIEW_LIMIT) return text

  const paragraphs = text.split(/\n{2,}/).map((p) => p.trim()).filter(Boolean)
  if (!paragraphs.length) return text.slice(0, PREVIEW_LIMIT).trim()

  const selected: string[] = []
  let count = 0
  const add = (p: string) => {
    if (!p || selected.includes(p)) return
    selected.push(p)
    count += p.length + 2
  }

  const hasKeyParagraph = paragraphs.some((p) => hasSummaryKeyword(p))
  if (hasKeyParagraph) {
    for (const p of paragraphs) {
      if (hasSummaryKeyword(p)) {
        add(p)
      }
      if (count >= PREVIEW_LIMIT) break
    }
    if (count < PREVIEW_MIN) {
      for (const p of paragraphs) {
        add(p)
        if (count >= PREVIEW_MIN) break
      }
    }
  } else {
    for (const p of paragraphs) {
      add(p)
      if (count >= PREVIEW_LIMIT) break
    }
    const keyLines = text
      .split('\n')
      .map((line) => line.trim())
      .filter((line) => line && PREVIEW_EXTRA_KEYS.some((k) => line.includes(k)))
    for (const line of keyLines) {
      if (count >= PREVIEW_LIMIT) break
      add(line)
    }
  }

  const preview = selected.join('\n\n').trim() || text.slice(0, PREVIEW_LIMIT).trim()
  return preview.length > PREVIEW_LIMIT ? preview.slice(0, PREVIEW_LIMIT).trim() : preview
}

export function formatAiForMobile(raw: string): MobileAiFormatted {
  const normalized = normalizeMarkdownToMobile(raw)
  if (!normalized) {
    return { fullText: '【AI生成】', previewText: '【AI生成】', hasMore: false }
  }

  const hasAiTag = normalized.startsWith('【AI生成】')
  const body = hasAiTag ? normalized.replace(/^【AI生成】\s*/, '').trim() : normalized.trim()
  const previewBody = extractPreviewBody(body) || body

  const fullText = hasAiTag ? `【AI生成】\n${body}`.trim() : body
  const previewText = hasAiTag ? `【AI生成】\n${previewBody}`.trim() : previewBody
  const hasMore = body.trim() !== previewBody.trim() && body.length > previewBody.length

  return { fullText, previewText, hasMore }
}

