import { fn } from "@/util/fn"
import z from "zod"
import { Session } from "."

import { MessageV2 } from "./message-v2"
import { Identifier } from "@/id/id"
import { Snapshot } from "@/snapshot"
import { Log } from "@/util/log"

import { Storage } from "@/storage/storage"
import { Bus } from "@/bus"

export namespace SessionSummary {
  const log = Log.create({ service: "session.summary" })
  function unquoteGitPath(input: string) {
    if (!input.startsWith('"')) return input
    if (!input.endsWith('"')) return input
    const body = input.slice(1, -1)
    const bytes: number[] = []

    for (let i = 0; i < body.length; i++) {
      const char = body[i]!
      if (char !== "\\") {
        bytes.push(char.charCodeAt(0))
        continue
      }

      const next = body[i + 1]
      if (!next) {
        bytes.push("\\".charCodeAt(0))
        continue
      }

      if (next >= "0" && next <= "7") {
        const chunk = body.slice(i + 1, i + 4)
        const match = chunk.match(/^[0-7]{1,3}/)
        if (!match) {
          bytes.push(next.charCodeAt(0))
          i++
          continue
        }
        bytes.push(parseInt(match[0], 8))
        i += match[0].length
        continue
      }

      const escaped =
        next === "n"
          ? "\n"
          : next === "r"
            ? "\r"
            : next === "t"
              ? "\t"
              : next === "b"
                ? "\b"
                : next === "f"
                  ? "\f"
                  : next === "v"
                    ? "\v"
                    : next === "\\" || next === '"'
                      ? next
                      : undefined

      bytes.push((escaped ?? next).charCodeAt(0))
      i++
    }

    return Buffer.from(bytes).toString()
  }

  // "Last-writer-wins" queue: ensures at most one summarize runs per session at
  // a time.  If a new request arrives while one is already running the latest
  // input is recorded and processed as soon as the current run completes.
  // Without this, every finish-step event fires a concurrent summarize, and
  // each one loads ALL session messages + runs expensive git operations.
  const queues = new Map<string, { latest: { sessionID: string; messageID: string } }>()

  export const summarize = fn(
    z.object({
      sessionID: z.string(),
      messageID: z.string(),
    }),
    (input) => {
      const entry = queues.get(input.sessionID)
      if (entry) {
        entry.latest = input // update; running call will re-run with this
        return
      }
      const q = { latest: input }
      queues.set(input.sessionID, q)
      void runSummary(q)
    },
  )

  async function runSummary(q: { latest: { sessionID: string; messageID: string } }) {
    const { sessionID } = q.latest
    try {
      while (true) {
        const current = q.latest
        const all = await Session.messages({ sessionID: current.sessionID })
        await summarizeSession({ sessionID: current.sessionID, messages: all })
        // If no newer request arrived while we were working, we're done.
        if (q.latest === current) break
      }
    } catch (err) {
      log.error("summarize failed", { sessionID, err })
    } finally {
      queues.delete(sessionID)
    }
  }

  async function summarizeSession(input: { sessionID: string; messages: MessageV2.WithParts[] }) {
    const diffs = await computeDiff({ messages: input.messages })
    await Session.setSummary({
      sessionID: input.sessionID,
      summary: {
        additions: diffs.reduce((sum, x) => sum + x.additions, 0),
        deletions: diffs.reduce((sum, x) => sum + x.deletions, 0),
        files: diffs.length,
      },
    })
    await Storage.write(["session_diff", input.sessionID], diffs)
    Bus.publish(Session.Event.Diff, {
      sessionID: input.sessionID,
      diff: diffs,
    })
  }

  export const diff = fn(
    z.object({
      sessionID: Identifier.schema("session"),
      messageID: Identifier.schema("message").optional(),
    }),
    async (input) => {
      const diffs = await Storage.read<Snapshot.FileDiff[]>(["session_diff", input.sessionID]).catch(() => [])
      const next = diffs.map((item) => {
        const file = unquoteGitPath(item.file)
        if (file === item.file) return item
        return {
          ...item,
          file,
        }
      })
      const changed = next.some((item, i) => item.file !== diffs[i]?.file)
      if (changed) Storage.write(["session_diff", input.sessionID], next).catch(() => {})
      return next
    },
  )

  export async function computeDiff(input: { messages: MessageV2.WithParts[] }) {
    let from: string | undefined
    let to: string | undefined

    // scan assistant messages to find earliest from and latest to
    // snapshot
    for (const item of input.messages) {
      if (!from) {
        for (const part of item.parts) {
          if (part.type === "step-start" && part.snapshot) {
            from = part.snapshot
            break
          }
        }
      }

      for (const part of item.parts) {
        if (part.type === "step-finish" && part.snapshot) {
          to = part.snapshot
        }
      }
    }

    if (from && to) return Snapshot.diffFull(from, to)
    return []
  }
}
