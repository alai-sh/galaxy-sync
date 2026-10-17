// Starts sync.yml every 10 minutes with a workflow_dispatch, because GitHub's
// own schedule trigger is late or skipped too often. Overlap is still handled
// by the workflow's concurrency group, and a sync paused by rebuild.yml (the
// workflow disabled) is skipped here. See "Scheduling" in docs/sync.md.

const API_VERSION = "2022-11-28";

export default {
  async scheduled(controller, env, ctx) {
    const workflow = `https://api.github.com/repos/${env.GITHUB_REPO}/actions/workflows/${env.WORKFLOW}`;
    const headers = {
      Accept: "application/vnd.github+json",
      Authorization: `Bearer ${env.GITHUB_TOKEN}`,
      "User-Agent": "alai-galaxy-sync-scheduler",
      "X-GitHub-Api-Version": API_VERSION,
    };

    const info = await fetch(workflow, { headers });
    if (!info.ok) {
      throw new Error(`reading ${env.WORKFLOW} failed: ${info.status} ${await info.text()}`);
    }
    const { state } = await info.json();
    if (state !== "active") {
      console.log(`${env.WORKFLOW} is ${state}; not starting it`);
      return;
    }

    const res = await fetch(`${workflow}/dispatches`, {
      method: "POST",
      headers,
      body: JSON.stringify({ ref: env.REF }),
    });
    if (!res.ok) {
      throw new Error(`dispatching ${env.WORKFLOW} failed: ${res.status} ${await res.text()}`);
    }
    console.log(`dispatched ${env.WORKFLOW} on ${env.REF}`);
  },
};
