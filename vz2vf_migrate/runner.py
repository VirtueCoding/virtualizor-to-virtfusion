import os
import shlex
import subprocess
from dataclasses import dataclass
from typing import Protocol


class Logger(Protocol):
    def log(self, message: str) -> None:
        pass


@dataclass(frozen=True)
class CommandResult:
    command: list[str]
    returncode: int
    stdout: str
    stderr: str
    skipped: bool


class CommandTimeoutError(RuntimeError):
    pass


class CommandRunner:
    def __init__(self, execute: bool, logger: Logger) -> None:
        self.execute = execute
        self.logger = logger

    def run(
        self,
        command: list[str],
        timeout: float | None = None,
        env: dict[str, str] | None = None,
    ) -> CommandResult:
        return self._invoke(command, force_execute=False, timeout=timeout, env=env)

    def run_with_input(
        self,
        command: list[str],
        input_text: str,
        timeout: float | None = None,
        env: dict[str, str] | None = None,
    ) -> CommandResult:
        return self._invoke(command, force_execute=False, input_text=input_text, timeout=timeout, env=env)

    def run_readonly(
        self,
        command: list[str],
        timeout: float | None = None,
        env: dict[str, str] | None = None,
    ) -> CommandResult:
        return self._invoke(command, force_execute=True, timeout=timeout, env=env)

    def run_readonly_with_input(
        self,
        command: list[str],
        input_text: str,
        timeout: float | None = None,
        env: dict[str, str] | None = None,
    ) -> CommandResult:
        return self._invoke(command, force_execute=True, input_text=input_text, timeout=timeout, env=env)

    def _invoke(
        self,
        command: list[str],
        force_execute: bool,
        input_text: str | None = None,
        timeout: float | None = None,
        env: dict[str, str] | None = None,
    ) -> CommandResult:
        rendered = " ".join(shlex.quote(part) for part in self._redact_command(command))
        self.logger.log(rendered)
        if not self.execute and not force_execute:
            return CommandResult(command=command, returncode=0, stdout="", stderr="", skipped=True)

        run_kwargs = {
            "capture_output": True,
            "check": False,
            "text": True,
        }
        if input_text is not None:
            run_kwargs["input"] = input_text
        if timeout is not None:
            run_kwargs["timeout"] = timeout
        if env is not None:
            run_kwargs["env"] = {**os.environ, **env}

        try:
            completed = subprocess.run(command, **run_kwargs)
        except subprocess.TimeoutExpired as exc:
            raise CommandTimeoutError(f"Command timed out after {exc.timeout}s: {rendered}") from exc
        return CommandResult(
            command=command,
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
            skipped=False,
        )

    @staticmethod
    def _redact_command(command: list[str]) -> list[str]:
        redacted: list[str] = []
        redact_next = False
        for part in command:
            if redact_next:
                redacted.append("REDACTED")
                redact_next = False
                continue
            if part.startswith("-p") and part != "-p":
                redacted.append("-pREDACTED")
                continue
            if part in {"-p", "--password"}:
                redacted.append(part)
                redact_next = True
                continue
            if part.startswith("--password="):
                redacted.append("--password=REDACTED")
                continue
            redacted.append(part)
        return redacted
