#!/usr/bin/env python3
"""
Run OpenEvolve to evolve the checkpoint scheduling algorithm.

This script initializes OpenEvolve with our configuration and runs
the evolution process to optimize the SchedulingAlgorithm.

Usage:
    # Run evolution with default settings
    python run_evolution.py

    # Run with custom iterations
    python run_evolution.py --iterations 50

    # Run with target score
    python run_evolution.py --target-score 80

    # Resume from checkpoint
    python run_evolution.py --resume output/checkpoint.json
"""

import argparse
import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from openevolve import OpenEvolve
from openevolve.config import Config


def get_default_paths():
    """Get default paths for evolution files."""
    evolve_dir = Path(__file__).parent

    return {
        'config': evolve_dir / 'config.yaml',
        'initial_program': evolve_dir / 'algorithm.py',
        'evaluation_file': evolve_dir / 'openevolve_eval.py',
        'output_dir': evolve_dir / 'evolution_output',
    }


def check_api_key():
    """Check that required API key is set."""
    if not os.environ.get('ANTHROPIC_API_KEY'):
        # Check for OpenAI as fallback
        if not os.environ.get('OPENAI_API_KEY'):
            print("Error: No API key found.")
            print("Please set ANTHROPIC_API_KEY or OPENAI_API_KEY environment variable.")
            print()
            print("Example:")
            print("  export ANTHROPIC_API_KEY='your-api-key-here'")
            print()
            return False
    return True


def setup_config(args, paths):
    """Load and customize configuration."""
    config = Config.from_yaml(paths['config'])

    # Override with command line arguments
    if args.iterations:
        config.max_iterations = args.iterations

    if args.seed:
        config.random_seed = args.seed

    if args.log_level:
        config.log_level = args.log_level

    # Setup API key
    api_key = os.environ.get('ANTHROPIC_API_KEY') or os.environ.get('OPENAI_API_KEY')
    if api_key:
        config.llm.api_key = api_key

    # Use OpenAI API if ANTHROPIC_API_KEY is not set
    if not os.environ.get('ANTHROPIC_API_KEY') and os.environ.get('OPENAI_API_KEY'):
        config.llm.api_base = 'https://api.openai.com/v1'
        config.llm.primary_model = 'gpt-4o'
        config.llm.secondary_model = 'gpt-4o-mini'
        config.llm.rebuild_models()

    return config


async def run_evolution(args):
    """Run the evolution process."""
    paths = get_default_paths()

    # Check prerequisites
    if not check_api_key():
        return None

    if not paths['initial_program'].exists():
        print(f"Error: Initial program not found: {paths['initial_program']}")
        return None

    if not paths['evaluation_file'].exists():
        print(f"Error: Evaluation file not found: {paths['evaluation_file']}")
        return None

    # Setup output directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = paths['output_dir'] / f"run_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("OpenEvolve - Checkpoint Scheduling Algorithm Evolution")
    print("=" * 60)
    print()
    print(f"Initial program: {paths['initial_program']}")
    print(f"Evaluation file: {paths['evaluation_file']}")
    print(f"Output directory: {output_dir}")
    print()

    # Load configuration
    config = setup_config(args, paths)
    print(f"Max iterations: {config.max_iterations}")
    print(f"Random seed: {config.random_seed}")
    print(f"Primary model: {config.llm.primary_model}")
    print()

    # Initialize OpenEvolve
    print("Initializing OpenEvolve...")
    evolve = OpenEvolve(
        initial_program_path=str(paths['initial_program']),
        evaluation_file=str(paths['evaluation_file']),
        config=config,
        output_dir=str(output_dir),
    )

    # Run evolution
    print("Starting evolution...")
    print("-" * 60)

    checkpoint_path = args.resume if args.resume else None

    best_program = await evolve.run(
        iterations=args.iterations,
        target_score=args.target_score,
        checkpoint_path=checkpoint_path,
    )

    print("-" * 60)
    print()

    if best_program:
        print("Evolution completed successfully!")
        print()
        print(f"Best score: {best_program.metrics.get('combined_score', 'N/A')}")
        print(f"Success rate: {best_program.metrics.get('success_rate', 'N/A'):.2%}")
        print(f"Overhead ratio: {best_program.metrics.get('overhead_ratio', 'N/A'):.2%}")
        print()
        print(f"Best program saved to: {output_dir / 'best_program.py'}")

        # Save the best program
        best_program_path = output_dir / 'best_program.py'
        with open(best_program_path, 'w') as f:
            f.write(best_program.code)

        return best_program
    else:
        print("Evolution completed but no improved program found.")
        return None


def main():
    parser = argparse.ArgumentParser(
        description='Run OpenEvolve to evolve checkpoint scheduling algorithm',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default settings (100 iterations)
  python run_evolution.py

  # Run with fewer iterations for testing
  python run_evolution.py --iterations 10

  # Run until target score is reached
  python run_evolution.py --target-score 80

  # Resume from checkpoint
  python run_evolution.py --resume evolution_output/run_xxx/checkpoint.json

Environment Variables:
  ANTHROPIC_API_KEY  - Anthropic API key (preferred)
  OPENAI_API_KEY     - OpenAI API key (fallback)
        """
    )

    parser.add_argument(
        '--iterations', '-n',
        type=int,
        help='Maximum number of iterations (default: from config)'
    )

    parser.add_argument(
        '--target-score',
        type=float,
        help='Target score to reach (evolution continues until reached)'
    )

    parser.add_argument(
        '--resume',
        type=str,
        help='Path to checkpoint file to resume from'
    )

    parser.add_argument(
        '--seed',
        type=int,
        help='Random seed for reproducibility'
    )

    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level'
    )

    args = parser.parse_args()

    # Run async evolution
    result = asyncio.run(run_evolution(args))

    return 0 if result else 1


if __name__ == '__main__':
    sys.exit(main())
