using System.CommandLine;
using System.CommandLine.Completions;

namespace DtPipe.Cli.Commands;

/// <summary>
/// Generates shell completion bootstrap scripts.
/// Supports: zsh, bash, powershell. Auto-detects shell if no argument given.
/// </summary>
public class CompletionCommand : Command
{
    // Markers used by --install / --uninstall to locate the block in shell profiles
    internal const string BlockBeginMarker = "# >>> dtpipe completion >>>";
    internal const string BlockEndMarker   = "# <<< dtpipe completion <<<";

    public CompletionCommand() : base("completion", "Generate or manage shell completion")
    {
        // Hidden = true; // No longer hidden, user wants to use it explicitly
        // Shell argument is OPTIONAL — auto-detected if omitted
        var shellArg = new Argument<string>("shell")
        {
            Description = "Target shell: bash, zsh, powershell (auto-detected if omitted)",
            DefaultValueFactory = _ => null!
        };
        shellArg.CompletionSources.Add("bash");
        shellArg.CompletionSources.Add("zsh");
        shellArg.CompletionSources.Add("powershell");
        Arguments.Add(shellArg);

        // Sub-commands for lifecycle management
        Subcommands.Add(CreateInstallCommand());
        Subcommands.Add(CreateUninstallCommand());

        this.SetAction((parseResult, ct) =>
        {
            var shell = parseResult.GetValue(shellArg) ?? DetectShell();
            var script = GenerateScript(shell);

            if (script == null)
            {
                var detected = DetectShell() ?? "unknown";
                Console.Error.WriteLine(
                    $"Could not generate completion for shell: '{shell ?? detected}'.\n" +
                    "Use: dtpipe completion [bash|zsh|powershell]");
                return Task.FromResult(1);
            }

            Console.WriteLine(script);
            return Task.FromResult(0);
        });
    }

    // ── Shell Detection ────────────────────────────────────────────────

    internal static string? DetectShell()
    {
        // PowerShell Core and Windows PowerShell both set $PSModulePath.
        // We check this first because on macOS/Linux, $SHELL often persists
        // even when running inside a pwsh subshell.
        if (Environment.GetEnvironmentVariable("PSModulePath") != null)
            return "powershell";

        var unixShell = Environment.GetEnvironmentVariable("SHELL");
        if (!string.IsNullOrEmpty(unixShell))
        {
            if (unixShell.Contains("zsh",  StringComparison.OrdinalIgnoreCase)) return "zsh";
            if (unixShell.Contains("bash", StringComparison.OrdinalIgnoreCase)) return "bash";
        }

        return null;
    }

    // ── Script Generation ──────────────────────────────────────────────

    internal static string? GenerateScript(string? shell) =>
        shell?.ToLowerInvariant() switch
        {
            "zsh"        => GenerateZshScript(),
            "bash"       => GenerateBashScript(),
            "powershell" => GeneratePowerShellScript(),
            _            => null
        };

    /// <summary>
    /// Generates the full bootstrap block (with guard) for the given shell.
    /// This is what --install writes into the shell profile file.
    /// </summary>
    internal static string? GenerateInstallBlock(string? shell)
    {
        var script = GenerateScript(shell);
        if (script == null) return null;

        var guardedScript = shell?.ToLowerInvariant() switch
        {
            "zsh" or "bash" =>
                $"if command -v \"${{DTPIPE_BIN:-dtpipe}}\" &>/dev/null; then\n" +
                $"{IndentLines(script, "  ")}\n" +
                $"else\n" +
                $"  echo \"[!] dtpipe not found. Reinstall: 'dotnet tool install -g dtpipe' or cleanup: 'dtpipe completion --uninstall'\"\n" +
                $"fi",
            "powershell" =>
                $"$dtpipeBin = if ($env:DTPIPE_BIN) {{ $env:DTPIPE_BIN }} else {{ 'dtpipe' }}\n" +
                $"if (Get-Command $dtpipeBin -ErrorAction SilentlyContinue) {{\n" +
                $"{IndentLines(script, "  ")}\n" +
                $"}} else {{\n" +
                $"  Write-Host \"[!] dtpipe not found — reinstall: 'dotnet tool install -g dtpipe' or cleanup: 'dtpipe completion --uninstall'\" -ForegroundColor Yellow\n" +
                $"}}",
            _ => null
        };

        if (guardedScript == null) return null;

        return $"{BlockBeginMarker}\n{guardedScript}\n{BlockEndMarker}";
    }

    private static string IndentLines(string text, string indent) =>
        string.Join('\n', text.Split('\n').Select(l => indent + l));

    private static string GenerateZshScript() => """
        _dtpipe() {
          local -a completions
          local -a filtered
          local bin="${DTPIPE_BIN:-$words[1]}"
          local nospace=0
          completions=( "${(@f)$($bin "[suggest]" $((CURRENT - 1)) "${words[@]:1}" 2>/dev/null)}" )
          for c in $completions; do
            if [[ "$c" == *"[NOSUSP]" ]]; then
              nospace=1
              filtered+=("${c%\[NOSUSP\]}")
            else
              filtered+=("$c")
            fi
          done
          if [[ $nospace -eq 1 ]]; then
            compadd -S "" -a filtered
          else
            compadd -a filtered
          fi
        }
        if (( ! $+functions[compdef] )); then
          autoload -Uz compinit && compinit
        fi
        if (( $+functions[compdef] )); then
          compdef _dtpipe dtpipe
          [[ -n "$DTPIPE_BIN" ]] && compdef _dtpipe "$DTPIPE_BIN"
        fi
        """.Replace("\r", "");

    private static string GenerateBashScript() => """
        _dtpipe_completion() {
          local cur="${COMP_WORDS[COMP_CWORD]}"
          local dtpipe_bin="${DTPIPE_BIN:-dtpipe}"
          local completions=$($dtpipe_bin [suggest] $COMP_CWORD "${COMP_WORDS[@]:1}" 2>/dev/null)
          if [[ "$completions" == *"[NOSUSP]"* ]]; then
            compopt -o nospace
            COMPREPLY=( $(echo "$completions" | sed 's/\[NOSUSP\]//g') )
          else
            COMPREPLY=( $completions )
          fi
        }
        complete -F _dtpipe_completion dtpipe
        """.Replace("\r", "");

    private static string GeneratePowerShellScript() => """
        Register-ArgumentCompleter -Native -CommandName dtpipe -ScriptBlock {
          param($wordToComplete, $commandAst, $cursorPosition)
          $bin = if ($env:DTPIPE_BIN) { $env:DTPIPE_BIN } else { 'dtpipe' }
          $pos = $commandAst.CommandElements.Count - 1
          $args = $commandAst.CommandElements | Select-Object -Skip 1 | ForEach-Object { $_.ToString() }
          & $bin [suggest] $pos @args 2>$null | ForEach-Object {
            $c = $_
            $type = [System.Management.Automation.CompletionResultType]::ParameterValue
            if ($c.EndsWith("[NOSUSP]")) {
              $c = $c.Substring(0, $c.Length - 8)
              $type = [System.Management.Automation.CompletionResultType]::IncompleteInput
            }
            [System.Management.Automation.CompletionResult]::new($c, $c, $type, $c)
          }
        }
        """.Replace("\r", "");

    // ── Lifecycle Sub-Commands ─────────────────────────────────────────

    private Command CreateInstallCommand()
    {
        var cmd = new Command("--install", "Install completion into shell profile");
        var shellOpt = new Option<string>("--shell") { Description = "Target shell (auto-detected if omitted)" };
        shellOpt.CompletionSources.Add("bash");
        shellOpt.CompletionSources.Add("zsh");
        shellOpt.CompletionSources.Add("powershell");
        cmd.Options.Add(shellOpt);

        cmd.SetAction((parseResult, ct) =>
        {
            var shell   = parseResult.GetValue(shellOpt) ?? DetectShell();
            var profile = GetProfilePath(shell);
            var block   = GenerateInstallBlock(shell);

            if (shell == null || profile == null || block == null)
            {
                Console.Error.WriteLine("Could not detect shell or profile path. Use --shell [bash|zsh|powershell]");
                return Task.FromResult(1);
            }

            // Idempotent: do not insert twice
            var existing = File.Exists(profile) ? File.ReadAllText(profile) : "";
            if (existing.Contains(BlockBeginMarker))
            {
                Console.Error.WriteLine($"Completion already installed in {profile}. Nothing to do.");
                Console.Error.WriteLine("To update: run 'dtpipe completion --uninstall' then 'dtpipe completion --install'.");
                return Task.FromResult(0);
            }

            if (!string.IsNullOrEmpty(profile))
            {
                var dir = Path.GetDirectoryName(profile);
                if (!string.IsNullOrEmpty(dir))
                {
                    if (!Directory.Exists(dir))
                    {
                        Console.Error.WriteLine($"Creating directory: {dir}");
                        Directory.CreateDirectory(dir);
                    }
                }
            }

            Console.Error.WriteLine($"Installing completion script to: {profile}");
            File.AppendAllText(profile!, $"\n{block}\n");
            Console.Error.WriteLine($"✅ Completion installed in {profile}");
            string sourceCmd = (shell?.ToLowerInvariant() == "powershell") ? ". " : "source ";
            Console.Error.WriteLine($"   To activate now, run: {sourceCmd}{profile}");
            return Task.FromResult(0);
        });

        return cmd;
    }

    private Command CreateUninstallCommand()
    {
        var cmd = new Command("--uninstall", "Remove completion from all known shell profiles");

        cmd.SetAction((parseResult, ct) =>
        {
            var profiles = new[]
            {
                GetProfilePath("zsh"),
                GetProfilePath("bash"),
                GetProfilePath("powershell"),
            }.OfType<string>().Distinct().ToList();

            var anyFound = false;
            foreach (var profile in profiles)
            {
                if (!File.Exists(profile)) continue;
                var content = File.ReadAllText(profile);
                if (!content.Contains(BlockBeginMarker)) continue;

                // Remove the block between the markers (inclusive)
                var updated = RemoveBlock(content);
                File.WriteAllText(profile, updated);
                Console.Error.WriteLine($"✅ Completion removed from {profile}");
                anyFound = true;
            }

            if (!anyFound)
                Console.Error.WriteLine("No dtpipe completion block found in known shell profiles.");

            return Task.FromResult(0);
        });

        return cmd;
    }

    // ── Shell Profile Paths ────────────────────────────────────────────

    private static string? GetProfilePath(string? shell)
    {
        var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        return shell?.ToLowerInvariant() switch
        {
            "zsh"        => Path.Combine(home, ".zshrc"),
            "bash"       => File.Exists(Path.Combine(home, ".bashrc"))
                                ? Path.Combine(home, ".bashrc")
                                : Path.Combine(home, ".bash_profile"),
            "powershell" => GetPowerShellProfilePath(),
            _            => null
        };
    }

    private static string GetPowerShellProfilePath()
    {
        var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);

        if (OperatingSystem.IsWindows())
        {
            // Windows: PowerShell Core
            var psCore = Path.Combine(home, "Documents", "PowerShell", "Microsoft.PowerShell_profile.ps1");
            if (File.Exists(psCore)) return psCore;

            // Windows: Windows PowerShell 5.1
            var psLegacy = Path.Combine(home, "Documents", "WindowsPowerShell", "Microsoft.PowerShell_profile.ps1");
            if (File.Exists(psLegacy)) return psLegacy;

            return psCore; // Default to Core
        }

        // Linux/macOS
        return Path.Combine(home, ".config", "powershell", "Microsoft.PowerShell_profile.ps1");
    }

    private static string RemoveBlock(string content)
    {
        var begin = content.IndexOf(BlockBeginMarker, StringComparison.Ordinal);
        var end   = content.IndexOf(BlockEndMarker,   StringComparison.Ordinal);
        if (begin < 0 || end < 0 || end < begin) return content;

        end += BlockEndMarker.Length;
        // Also consume a trailing newline if present
        if (end < content.Length && content[end] == '\n') end++;

        return content[..begin] + content[end..];
    }
}
