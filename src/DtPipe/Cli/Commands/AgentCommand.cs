using System;
using System.CommandLine;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Mcp;
using Microsoft.Extensions.DependencyInjection;
using Spectre.Console;

namespace DtPipe.Cli.Commands;

public class AgentCommand : Command
{
    public AgentCommand(IServiceProvider serviceProvider) 
        : base("agent", "Start an interactive or automated ReAct AI agent loop for data integration tasks")
    {
        var promptArgument = new Argument<string?>("prompt")
        {
            Description = "The data integration task description (e.g. 'Inspect csv:invoices.csv and anonymize email')",
            Arity = ArgumentArity.ZeroOrOne
        };

        var promptOption = new Option<string?>("--prompt")
        {
            Description = "The data integration task description"
        };
        promptOption.Aliases.Add("-p");

        var providerOption = new Option<string>("--provider")
        {
            Description = "LLM provider to use ('ollama' or 'openai')"
        };
        providerOption.DefaultValueFactory = _ => "ollama";

        var apiKeyOption = new Option<string?>("--api-key")
        {
            Description = "API key for OpenAI provider. Falls back to DTPIPE_LLM_API_KEY environment variable."
        };

        var modelOption = new Option<string?>("--model")
        {
            Description = "Model name (e.g. 'qwen2.5-coder:7b', 'gpt-4o'). Auto-discovered if omitted."
        };
        modelOption.Aliases.Add("-m");

        var urlOption = new Option<string?>("--url")
        {
            Description = "API endpoint URL (defaults: http://localhost:11434 for ollama, https://api.openai.com for openai)"
        };
        urlOption.Aliases.Add("-u");

        var maxIterOption = new Option<int>("--max-iterations")
        {
            Description = "Maximum ReAct loop iterations per turn"
        };
        maxIterOption.DefaultValueFactory = _ => 25;

        var llmTimeoutOption = new Option<int>("--llm-timeout")
        {
            Description = "Seconds to wait for a single LLM response before giving up. When streaming, this is the max silence between tokens (default: 300)."
        };
        llmTimeoutOption.DefaultValueFactory = _ => 300;

        var noStreamOption = new Option<bool>("--no-stream")
        {
            Description = "Disable token streaming and its live view; use one blocking call per step."
        };
        noStreamOption.DefaultValueFactory = _ => false;

        var showThinkingOption = new Option<bool>("--show-thinking")
        {
            Description = "Alias for --detail full: keep the model's full chain of thought on screen (implied by DEBUG=1)."
        };
        showThinkingOption.DefaultValueFactory = _ => false;

        var detailOption = new Option<AgentDetailLevel>("--detail")
        {
            Description = "How much of each step stays in scrollback: 'compact' (default: trace line + stated intent), "
                + "'peek' (+ a chain-of-thought preview), 'full' (+ the whole chain of thought)."
        };
        detailOption.DefaultValueFactory = _ => AgentDetailLevel.Compact;

        var numCtxOption = new Option<int>("--num-ctx")
        {
            Description = $"Model context window to request from the provider (default: {AgentOptions.DefaultNumCtx})."
        };
        numCtxOption.DefaultValueFactory = _ => AgentOptions.DefaultNumCtx;

        var interactiveOption = new Option<bool>("--interactive")
        {
            Description = "Force interactive mode for model selection and task prompt"
        };
        interactiveOption.Aliases.Add("-i");

        var temperatureOption = new Option<double>("--temperature")
          {
            Description = "Sampling temperature. 0 makes decoding deterministic (default)."
          };
        temperatureOption.DefaultValueFactory = _ => 0.0;

        var seedOption = new Option<int?>("--seed")
          {
            Description = "Fixed seed for reproducible sampling. A fixed seed makes the run deterministic."
          };
        seedOption.DefaultValueFactory = _ => 0;

        var repeatOption = new Option<int>("--repeat")
           {
            Description = "Number of replications of the validated plan for determinism/variance measurement (default: 1)."
           };
        repeatOption.DefaultValueFactory = _ => 1;

        var sequentialOption = new Option<bool>("--sequential")
            {
            Description = "Execute tool calls one at a time instead of running independent calls in parallel (default: parallel)."
            };
        sequentialOption.DefaultValueFactory = _ => false;

        var modeOption = new Option<AgentMode>("--mode")
              {
             Description = "Operating mode. 'plan' (default) only designs & validates a pipeline; 'execute'/'autonomous' may run it through the guardrails."
              };
        modeOption.DefaultValueFactory = _ => AgentMode.Plan;

        var applyOption = new Option<bool>("--apply")
              {
             Description = "Perform a real write when executing a pipeline. Default (off) => dry-run only; writes also require approval."
              };
        applyOption.DefaultValueFactory = _ => false;

        var allowDestructiveOption = new Option<bool>("--allow-destructive")
              {
             Description = "Allow destructive SQL verbs (DROP/DELETE/TRUNCATE/UPDATE/ALTER/INSERT/ATTACH). Default deny."
              };
        allowDestructiveOption.DefaultValueFactory = _ => false;

        var allowNetworkOption = new Option<bool>("--allow-network")
              {
             Description = "Allow network access in SQL (LOAD httpfs/azure, remote read_parquet). Default deny."
              };
        allowNetworkOption.DefaultValueFactory = _ => false;

        Arguments.Add(promptArgument);
        Options.Add(promptOption);
        Options.Add(providerOption);
        Options.Add(apiKeyOption);
        Options.Add(modelOption);
        Options.Add(urlOption);
        Options.Add(maxIterOption);
        Options.Add(llmTimeoutOption);
        Options.Add(noStreamOption);
        Options.Add(showThinkingOption);
        Options.Add(detailOption);
        Options.Add(numCtxOption);
        Options.Add(interactiveOption);
        Options.Add(temperatureOption);
        Options.Add(seedOption);
        Options.Add(repeatOption);
        Options.Add(sequentialOption);
        Options.Add(modeOption);
        Options.Add(applyOption);
        Options.Add(allowDestructiveOption);
        Options.Add(allowNetworkOption);

        this.SetAction(async (parseResult, ct) =>
        {
            var console = serviceProvider.GetRequiredService<IAnsiConsole>();
            var mcpTools = serviceProvider.GetRequiredService<DtPipeMcpTools>();

            var prompt = parseResult.GetValue(promptArgument) ?? parseResult.GetValue(promptOption);
            var provider = parseResult.GetValue(providerOption) ?? "ollama";
            var apiKey = parseResult.GetValue(apiKeyOption);
            var model = parseResult.GetValue(modelOption);
            var url = parseResult.GetValue(urlOption);
            var maxIterations = parseResult.GetValue(maxIterOption);
            var llmTimeout = TimeSpan.FromSeconds(Math.Max(1, parseResult.GetValue(llmTimeoutOption)));
            var noStream = parseResult.GetValue(noStreamOption);
            var detail = parseResult.GetValue(detailOption);
            if (parseResult.GetValue(showThinkingOption) || Environment.GetEnvironmentVariable("DEBUG") == "1")
                detail = AgentDetailLevel.Full;
            var numCtx = Math.Max(2048, parseResult.GetValue(numCtxOption));
            var temperature = parseResult.GetValue(temperatureOption);
            var seed = parseResult.GetValue(seedOption);
             var repeat = parseResult.GetValue(repeatOption);
              var sequential = parseResult.GetValue(sequentialOption);
              var mode = parseResult.GetValue(modeOption);
              var apply = parseResult.GetValue(applyOption);
              var allowDestructive = parseResult.GetValue(allowDestructiveOption);
              var allowNetwork = parseResult.GetValue(allowNetworkOption);

            if (string.IsNullOrWhiteSpace(url))
            {
                url = provider.Equals("openai", StringComparison.OrdinalIgnoreCase)
                    ? "https://api.openai.com"
                    : "http://localhost:11434";
            }

            var tui = new AgentTui(console);
            ILlmClient llmClient = provider.Equals("openai", StringComparison.OrdinalIgnoreCase)
                ? new OpenAiClient(apiKey, llmTimeout)
                : new OllamaClient(llmTimeout);

            if (string.IsNullOrWhiteSpace(model))
            {
                model = await tui.SelectModelAsync(llmClient, url);
                if (string.IsNullOrWhiteSpace(model))
                {
                    console.MarkupLine("[red]Error:[/] No model selected.");
                    return 1;
                }
            }

            if (string.IsNullOrWhiteSpace(prompt))
            {
                prompt = tui.PromptUserMission();
                if (string.IsNullOrWhiteSpace(prompt))
                {
                    console.MarkupLine("[red]Error:[/] Mission prompt cannot be empty.");
                    return 1;
                }
            }

            tui.RenderRunContext(model, url, mode, detail);

            var toolProvider = new McpToolProvider(mcpTools);
            var executor = new AgentExecutor(toolProvider, llmClient, tui, console);

              var agentOptions = new AgentOptions
                      {
                Mode = mode,
                Temperature = temperature,
                Seed = seed,
                Repeat = repeat,
                 Sequential = sequential,
                Apply = apply,
                AllowDestructive = allowDestructive,
                AllowNetwork = allowNetwork,
                NoStream = noStream,
                Detail = detail,
                NumCtx = numCtx
                       };

                // F2: share the agent guardrail options with the execution tool so execute-yaml-job
               // honors --apply / --allow-destructive / --allow-network (fail-closed by default).
              mcpTools.AgentOptions = agentOptions;

                // Execute initial turn
              var exitCode = 0;
              try
              {
                  exitCode = await executor.RunTurnAsync(prompt, model, url, agentOptions, maxIterations, ct);

                  // The post-mission menu is an ANSI selection prompt; it throws on a piped or
                  // dumb terminal (CI, `script`, a redirect). A one-shot run there is complete —
                  // its exit code stands.
                  bool interactive = console.Profile.Capabilities.Interactive
                      && console.Profile.Capabilities.Ansi
                      && !Console.IsOutputRedirected && !Console.IsInputRedirected;

                  // Interactive post-mission conversation loop
                  while (interactive && !ct.IsCancellationRequested)
                  {
                      bool hasYaml = !string.IsNullOrEmpty(executor.Trajectory.LastGeneratedYaml);
                      PostMissionAction action;
                      try
                      {
                          action = tui.ShowPostMissionMenu(hasYaml, agentOptions.Apply, executor.Mode);
                      }
                      catch (Exception ex) when (ex is NotSupportedException or InvalidOperationException)
                      {
                          // The terminal cannot host the prompt after all — stop cleanly.
                          break;
                      }

                      if (action == PostMissionAction.Exit)
                      {
                          break;
                      }

                      switch (action)
                      {
                           case PostMissionAction.ContinueDiscussion:
                               var followUp = tui.PromptFollowUp();
                               if (!string.IsNullOrWhiteSpace(followUp))
                                {
                                    exitCode = await executor.RunTurnAsync(followUp, model, url, agentOptions, maxIterations, ct);
                                }
                               break;

                          case PostMissionAction.SwitchMode:
                              // F1 stays a per-turn invariant: the next RunTurnAsync rebuilds the
                              // role prompt and tool allow-list from executor.Mode. No write gate moves.
                              tui.RenderModeSwitch(executor.CycleMode(), agentOptions.Apply);
                              break;

                          case PostMissionAction.ExecutePlan:
                              if (!string.IsNullOrEmpty(executor.Trajectory.LastGeneratedYaml))
                               {
                                   // Deterministic: the reviewed YAML runs straight through the engine,
                                   // not back through the model. The tool's own F2 guardrails apply —
                                   // without --apply this is a sample run with the writer neutralised.
                                   tui.RenderPipelineDag(executor.Trajectory.LastGeneratedYaml, serviceProvider);
                                   if (!agentOptions.Apply || tui.ConfirmRealWrite())
                                    {
                                        var result = await executor.ExecuteValidatedPlanAsync(ct);
                                        tui.RenderExecutionResult(result.Content, result.IsError);
                                    }
                               }
                              break;

                          case PostMissionAction.ViewDag:
                              if (executor.Trajectory.LastGeneratedYaml != null)
                              {
                                  tui.RenderPipelineDag(executor.Trajectory.LastGeneratedYaml, serviceProvider);
                              }
                              break;

                          case PostMissionAction.InspectTrajectory:
                              tui.InspectTrajectory(executor.Trajectory);
                              break;

                          case PostMissionAction.SaveYaml:
                              if (executor.Trajectory.LastGeneratedYaml != null)
                              {
                                  tui.SaveYamlToFile(executor.Trajectory.LastGeneratedYaml);
                              }
                              break;
                      }
                  }

                  return exitCode;
              }
              catch (OperationCanceledException)
              {
                  // F16: user cancellation must not mask as success — report the POSIX SIGINT
                  // convention. A slow or dead endpoint no longer lands here: the LLM clients turn
                  // a request timeout into a stated error instead of a bare cancellation.
                  console.MarkupLine("[yellow]Interrupted — stopping.[/]");
                  return 130;
              }
          });
    }
}
