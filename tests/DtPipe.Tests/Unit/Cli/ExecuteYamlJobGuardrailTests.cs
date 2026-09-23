using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using DtPipe.Cli.Mcp;
using DtPipe.Cli.Security;
using DtPipe.Core.Abstractions;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// F2 — execute-yaml-job guardrails: fail-closed by default (dry-run), destructive SQL / network
/// blocked, writes refused without approval. These paths short-circuit before the JobService.
/// </summary>
public class ExecuteYamlJobGuardrailTests
{
     private static (DtPipeMcpTools tools, IServiceProvider sp) BuildTools()
         {
         var services = new ServiceCollection();
         services.AddSingleton<DtPipe.Core.Options.OptionsRegistry>();
         services.AddSingleton<IEnumerable<IStreamTransformerFactory>>(Array.Empty<IStreamTransformerFactory>());
         services.AddSingleton<IEnumerable<IStreamReaderFactory>>(Array.Empty<IStreamReaderFactory>());
         services.AddSingleton<IEnumerable<IDataWriterFactory>>(Array.Empty<IDataWriterFactory>());
         services.AddSingleton<IMcpHelpService, McpHelpService>();
         services.AddSingleton<DtPipeMcpTools>(sp =>
             new DtPipeMcpTools(
                sp.GetRequiredService<IEnumerable<IStreamReaderFactory>>(),
                Array.Empty<IDataTransformerFactory>(),
                sp.GetRequiredService<IEnumerable<IDataWriterFactory>>(),
                sp.GetRequiredService<IMcpHelpService>(),
                sp));

         var sp = services.BuildServiceProvider();
         return (sp.GetRequiredService<DtPipeMcpTools>(), sp);
          }

      [Fact]
     public async System.Threading.Tasks.Task Apply_False_Returns_DryRun_And_Writes_Nothing()
         {
         var (tools, _) = BuildTools();
         var yaml = "main:\n  input: \"csv:in.csv\"\n  output: \"csv:out.csv\"\n";

         var json = await tools.ExecuteYamlJob(yaml);

         // apply=false now runs a real sample with the writer neutralised, instead of
         // executing nothing. Fail-closed is unchanged and still legible: whatever the run's
         // outcome, this call did not consent to a write.
         Assert.Contains("\"applied\": false", json);

         // Honest is not the same as usable. A response reporting the run without saying what
         // is left to do reads as "the job is done" — a real agent mission stopped exactly here
         // and never wrote its target. So the remedy is in the payload on EVERY apply=false
         // path, including the ones where the run itself failed: a model told only that
         // something went wrong cannot tell whether writing is still pending.
         Assert.Contains("apply=true", json);
         Assert.Contains("NOTHING WAS WRITTEN", json);
            }

        [Fact]
     public async System.Threading.Tasks.Task Destructive_Sql_IsBlocked_Without_AllowDestructive()
         {
         var (tools, _) = BuildTools();
         var yaml = "main:\n  input: \"duck:m.db\"\n  output: \"null:\"\n  provider-options:\n    duck:\n      pre-exec: \"DROP TABLE sales\"\n";

         var json = await tools.ExecuteYamlJob(yaml);
         Assert.Contains("\"stage\": \"safety\"", json);
         Assert.Contains("\"success\": false", json);
            }

         [Fact]
    public async System.Threading.Tasks.Task Destructive_Sql_Allowed_With_Flag()
             {
             var (tools, _) = BuildTools();
             var yaml = "main:\n  input: \"duck:m.db\"\n  output: \"null:\"\n  provider-options:\n    duck:\n      pre-exec: \"DROP TABLE sales\"\n";

             // allowDestructive=true should clear the safety stage; the run then proceeds as a
             // dry-run because apply defaults to false.
            var json = await tools.ExecuteYamlJob(yaml, apply: false, allowDestructive: true);
            Assert.DoesNotContain("\"stage\": \"safety\"", json);
              }

         [Fact]
    public async System.Threading.Tasks.Task Network_Sql_IsBlocked_Without_AllowNetwork()
             {
             var (tools, _) = BuildTools();
             var yaml = "main:\n  input: \"duck:m.db\"\n  output: \"null:\"\n  provider-options:\n    duck:\n      duck-init: \"LOAD httpfs; SET s3_region='eu'\"\n";

             var json = await tools.ExecuteYamlJob(yaml);
             Assert.Contains("\"stage\": \"safety\"", json);
             Assert.Contains("\"success\": false", json);
              }

        [Fact]
     public async System.Threading.Tasks.Task Empty_Yaml_Returns_Error()
         {
         var (tools, _) = BuildTools();
         var json = await tools.ExecuteYamlJob("");
         Assert.Contains("YAML job content cannot be empty", json);
            }

         // Helper: the JSON is serialized with escaped quotes (\\"). Normalize so substring
          // assertions can use single quotes for readability.
        private static string ReplaceDoubleWithSingle(string json)
             => json.Replace("\"", "'");
        }
