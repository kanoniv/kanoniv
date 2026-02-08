use clap::{Parser, Subcommand};
use colored::Colorize;
use std::path::PathBuf;

mod commands;
mod parser;
mod validator;

#[derive(Parser)]
#[command(name = "kanoniv")]
#[command(author = "Kanoniv <oss@kanoniv.com>")]
#[command(version = "0.1.0")]
#[command(about = "Validate and compile Kanoniv identity specifications", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Validate a Kanoniv identity specification
    Validate {
        /// Path to the YAML file
        #[arg(value_name = "FILE")]
        file: PathBuf,

        /// Output format (text, json)
        #[arg(short, long, default_value = "text")]
        format: String,
    },

    /// Compile a specification to intermediate representation
    Compile {
        /// Path to the YAML file
        #[arg(value_name = "FILE")]
        file: PathBuf,

        /// Output file path
        #[arg(short, long)]
        output: Option<PathBuf>,
    },

    /// Compute the plan hash for a specification
    Hash {
        /// Path to the YAML file
        #[arg(value_name = "FILE")]
        file: PathBuf,
    },

    /// Compare two specification versions
    Diff {
        /// First version
        #[arg(value_name = "FILE1")]
        file1: PathBuf,

        /// Second version
        #[arg(value_name = "FILE2")]
        file2: PathBuf,
    },
    /// Generate an execution plan for a specification
    Plan {
        /// Path to the YAML file
        #[arg(value_name = "FILE")]
        file: PathBuf,
    },
}

fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Validate { file, format } => commands::validate::run(&file, &format),
        Commands::Compile { file, output } => commands::compile::run(&file, output.as_deref()),
        Commands::Hash { file } => commands::hash::run(&file),
        Commands::Diff { file1, file2 } => commands::diff::run(&file1, &file2),
        Commands::Plan { file } => commands::plan::run(&file),
    };

    match result {
        Ok(_) => std::process::exit(0),
        Err(e) => {
            eprintln!("{} {}", "error:".red().bold(), e);
            std::process::exit(1);
        }
    }
}
