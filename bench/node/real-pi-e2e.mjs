import { LLMock } from "@copilotkit/llmock";
import { spawn } from "node:child_process";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import readline from "node:readline";

const adapter = resolve(
	process.env.PI_ACP_BIN ?? "../../target/debug/pi-acp",
);
const pi = process.env.PI_ACP_REAL_PI;
if (!pi) {
	throw new Error("PI_ACP_REAL_PI must point to a real Pi CLI executable");
}

const mock = new LLMock({ port: 0, logLevel: "silent" });
mock.addFixtures([
	{ match: { predicate: () => true }, response: { content: "PONG" } },
]);
const url = await mock.start();
const home = mkdtempSync(join(tmpdir(), "pi-acp-home-"));
const state = mkdtempSync(join(tmpdir(), "pi-acp-state-"));
mkdirSync(join(home, ".pi", "agent"), { recursive: true });
writeFileSync(
	join(home, ".pi", "agent", "models.json"),
	JSON.stringify({
		providers: { anthropic: { baseUrl: url, apiKey: "mock-key" } },
	}),
);

const child = spawn(
	adapter,
	["--pi-command", pi, "--state-dir", state],
	{
		cwd: resolve("../.."),
		env: {
			...process.env,
			HOME: home,
			ANTHROPIC_API_KEY: "mock-key",
			ANTHROPIC_BASE_URL: url,
			PI_OFFLINE: "1",
		},
		stdio: ["pipe", "pipe", "pipe"],
	},
);

const lines = readline.createInterface({ input: child.stdout });
const pending = new Map();
const notifications = [];
let nextId = 1;
let stderr = "";
child.stderr.on("data", (chunk) => {
	stderr += chunk;
});
lines.on("line", (line) => {
	const message = JSON.parse(line);
	const key = String(message.id);
	if (message.id != null && pending.has(key)) {
		pending.get(key)(message);
		pending.delete(key);
	} else {
		notifications.push(message);
	}
});

async function request(method, params, timeoutMs = 60_000) {
	const id = nextId++;
	const response = await new Promise((resolveResponse, reject) => {
		pending.set(String(id), resolveResponse);
		child.stdin.write(
			`${JSON.stringify({ jsonrpc: "2.0", id, method, params })}\n`,
		);
		setTimeout(() => {
			if (pending.delete(String(id))) {
				reject(new Error(`timed out waiting for ${method}`));
			}
		}, timeoutMs).unref();
	});
	if (response.error) {
		throw new Error(`${method} failed: ${JSON.stringify(response.error)}`);
	}
	return response.result;
}

function streamedText(start) {
	return notifications
		.slice(start)
		.map((message) => message?.params?.update?.content?.text ?? "")
		.join("");
}

try {
	await request("initialize", {
		protocolVersion: 1,
		clientCapabilities: {},
	});
	const created = await request("session/new", {
		cwd: resolve("../.."),
		mcpServers: [],
	});
	const sessionId = created.sessionId;

	const firstStart = notifications.length;
	const first = await request("session/prompt", {
		sessionId,
		prompt: [{ type: "text", text: "reply" }],
	});
	const firstText = streamedText(firstStart);
	if (first.stopReason !== "end_turn" || firstText !== "PONG") {
		throw new Error(`unexpected first turn: ${JSON.stringify({ first, firstText })}`);
	}

	await request("session/close", { sessionId });
	const resumed = await request("session/resume", {
		sessionId,
		cwd: resolve("../.."),
		mcpServers: [],
	});
	const secondStart = notifications.length;
	const second = await request("session/prompt", {
		sessionId,
		prompt: [{ type: "text", text: "reply again" }],
	});
	const secondText = streamedText(secondStart);
	if (second.stopReason !== "end_turn" || secondText !== "PONG") {
		throw new Error(
			`unexpected resumed turn: ${JSON.stringify({ second, secondText })}`,
		);
	}

	const modelOption = created.configOptions.find((option) => option.id === "model");
	const models = modelOption.options.reduce(
		(count, group) => count + group.options.length,
		0,
	);
	console.log(
		JSON.stringify({
			piVersion: process.env.PI_ACP_PI_VERSION ?? "unknown",
			models,
			firstText,
			secondText,
			resumeConfigOptions: resumed.configOptions.length,
			llmRequests: mock.getRequests().length,
		}),
	);
} finally {
	child.kill("SIGTERM");
	await mock.stop();
	rmSync(home, { recursive: true, force: true });
	rmSync(state, { recursive: true, force: true });
	if (stderr) process.stderr.write(stderr);
}
