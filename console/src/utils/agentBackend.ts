import type { AgentBackend, AgentSummary } from "../api/types/agents";
import type { HarnessCapabilities } from "../api/modules/harness";

const BACKEND_DISPLAY_NAMES: Record<string, string> = {
  qwenpaw: "QwenPaw",
  codex: "Codex",
  qoder: "Qoder",
  minimax: "MiniMax Code",
};

export function backendDisplayName(backend: AgentBackend): string {
  return BACKEND_DISPLAY_NAMES[backend] ?? backend;
}

export function requiresQwenPawModel(backend: AgentBackend): boolean {
  return backend === "qwenpaw";
}

export function supportsPortabilityImport(agent?: AgentSummary): boolean {
  return (
    agent?.backend === "qwenpaw" &&
    agent.backend_capabilities?.workspace_ui !== false
  );
}

export function supportsAgentAttachments(
  backend: AgentBackend,
  capabilities?: Partial<HarnessCapabilities>,
): boolean {
  return requiresQwenPawModel(backend) || Boolean(capabilities?.attachments);
}
