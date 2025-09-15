import argparse
import re

from llm_prompt import LLMPrompt
import prompt

system_role = "You are a professional software engineer who understands Java projects and various configuration items, and you know how to write professional test code."


def config_summary(model: str = "kimi") -> str:
    llm_prompt = LLMPrompt(model)
    summary = llm_prompt.ask_LLM(prompt.config_summary, system_role)
    print(summary)
    return summary


def config_functional_testing(model: str = "kimi") -> None:
    llm_prompt = LLMPrompt(model)
    test_description = llm_prompt.ask_LLM(prompt.config_functional_testing_stage_1, system_role)
    print(test_description)

    summary = config_summary(model)

    cases = test_description
    if isinstance(test_description, str):
        lines = [ln.strip("-•* ").strip() for ln in test_description.splitlines()]
        cases = [ln for ln in lines if ln]

    for case in cases:
        prompt_code = (
            prompt.config_functional_testing_stage_2
            .replace("{test_case}", str(case))
            .replace("{config_summary}", summary)
        )
        test_code = llm_prompt.ask_LLM(prompt_code, system_role)
        print(test_code)


def config_validity_testing(model: str = "kimi") -> None:
    llm_prompt = LLMPrompt(model)
    summary = config_summary(model)
    prompt_code = prompt.value_validity_testing.replace("{config_summary}", summary)
    res = llm_prompt.ask_LLM(prompt_code, system_role)
    print(res)


def main():
    parser = argparse.ArgumentParser(
        description="Select which function to run for config-related LLM workflows."
    )
    parser.add_argument(
        "command",
        choices=["summary", "functional", "validity", "all"],
        help="Choose what to run."
    )
    parser.add_argument(
        "--model", "-m",
        default="kimi",
        help="LLM model name (default: kimi)."
    )

    args = parser.parse_args()

    if args.command == "summary":
        config_summary(args.model)
    elif args.command == "functional":
        config_functional_testing(args.model)
    elif args.command == "validity":
        config_validity_testing(args.model)
    elif args.command == "all":
        config_summary(args.model)
        config_functional_testing(args.model)
        config_validity_testing(args.model)


if __name__ == "__main__":
    main()
