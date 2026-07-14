use std::{path::PathBuf, sync::Arc};

use clap::Parser;
use pi_acp_rust::{Adapter, AdapterConfig};

#[derive(Debug, Parser)]
#[command(
    name = "pi-acp",
    version,
    about = "ACP adapter for the Pi coding agent"
)]
struct Args {
    #[arg(long, env = "PI_ACP_PI_COMMAND", default_value = "pi")]
    pi_command: String,

    #[arg(long, env = "PI_ACP_STATE_DIR")]
    state_dir: Option<PathBuf>,

    #[arg(long = "append-system-prompt")]
    append_system_prompts: Vec<String>,

    #[arg(long, default_value_t = 32)]
    max_queued_prompts: usize,

    #[arg(long, default_value_t = 512)]
    max_tracked_tool_calls: usize,

    #[arg(long = "pi-arg")]
    pi_args: Vec<String>,

    #[arg(long)]
    terminal_login: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "pi_acp=warn".into()),
        )
        .init();
    let args = Args::parse();

    if args.terminal_login {
        let status = tokio::process::Command::new(&args.pi_command)
            .status()
            .await?;
        std::process::exit(status.code().unwrap_or(1));
    }

    let state_dir = args.state_dir.unwrap_or_else(|| {
        directories::ProjectDirs::from("dev", "rivet", "pi-acp")
            .map(|dirs| {
                dirs.state_dir()
                    .unwrap_or_else(|| dirs.data_local_dir())
                    .to_path_buf()
            })
            .unwrap_or_else(|| PathBuf::from(".pi-acp"))
    });

    let backend: Arc<dyn pi_acp_rust::process::ProcessBackend> =
        Arc::new(pi_acp_rust::process::NativeProcessBackend);

    let adapter = Adapter::new(
        AdapterConfig {
            pi_command: args.pi_command,
            state_dir,
            append_system_prompts: args.append_system_prompts,
            pi_args: args.pi_args,
            max_queued_prompts: args.max_queued_prompts,
            max_tracked_tool_calls: args.max_tracked_tool_calls,
        },
        backend,
    );

    adapter
        .serve(tokio::io::stdin(), tokio::io::stdout())
        .await?;
    Ok(())
}
