using System.CommandLine;
using QueryDump.Configuration;

namespace QueryDump.Core;

public class TransformerPipelineBuilder
{
    private readonly IEnumerable<IDataTransformerFactory> _factories;

    public TransformerPipelineBuilder(IEnumerable<IDataTransformerFactory> factories)
    {
        _factories = factories;
    }

    public List<IDataTransformer> Build(string[] args)
    {
        var instructions = ParseInstructions(args);
        var pipeline = new List<IDataTransformer>();

        // Group consecutive instructions by Factory type
        var groups = GroupConsecutive(instructions);

        foreach (var group in groups)
        {
            var factory = group.Factory;
            var config = group.Configuration;
            
            // Instantiate transformer with collected values
            // Instantiate transformer with collected values
            var transformer = factory.CreateFromConfiguration(group.Configuration);
            if (transformer != null)
            {
                pipeline.Add(transformer);
            }
        }

        return pipeline;
    }

    private record Instruction(IDataTransformerFactory Factory, string Option, string Value);
    private record InstructionGroup(IDataTransformerFactory Factory, List<(string Option, string Value)> Configuration);

    private List<Instruction> ParseInstructions(string[] args)
    {
        var instructions = new List<Instruction>();
        
        // Map option aliases to (Factory, Option) tuple
        // We need to know which CLI option corresponds to which factory AND access the Option metadata (Arity)
        var optionMap = new Dictionary<string, (IDataTransformerFactory Factory, Option Option)>(StringComparer.OrdinalIgnoreCase);
        
        foreach (var factory in _factories)
        {
            foreach (var option in factory.GetCliOptions())
            {
                if (!string.IsNullOrEmpty(option.Name))
                {
                    optionMap[option.Name] = (factory, option);
                }
                foreach (var alias in option.Aliases)
                {
                    optionMap[alias] = (factory, option);
                }
            }
        }

        for (int i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            
            // Is this a transformer option?
            if (optionMap.TryGetValue(arg, out var match))
            {
                var factory = match.Factory;
                var option = match.Option;
                
                // Check Arity to determine if we should consume the next argument
                // For flags (Arity 0), we assume value is "true" and don't consume next arg
                // For values (Arity > 0), we consume next arg
                
                if (option.Arity.MaximumNumberOfValues > 0)
                {
                    // Expect value
                    if (i + 1 < args.Length)
                    {
                        var value = args[i + 1];
                        instructions.Add(new Instruction(factory, arg, value));
                        i++; // Skip next arg since we consumed it
                    }
                }
                else
                {
                    // Flag (boolean) -> Assume true, do NOT consume next arg
                    instructions.Add(new Instruction(factory, arg, "true"));
                }
            }
        }

        return instructions;
    }

    private static List<InstructionGroup> GroupConsecutive(List<Instruction> instructions)
    {
        var groups = new List<InstructionGroup>();
        if (instructions.Count == 0) return groups;

        InstructionGroup? currentGroup = null;

        foreach (var instr in instructions)
        {
            if (currentGroup == null || currentGroup.Factory != instr.Factory)
            {
                currentGroup = new InstructionGroup(instr.Factory, new List<(string Option, string Value)>());
                groups.Add(currentGroup);
            }
            
            currentGroup.Configuration.Add((instr.Option, instr.Value));
        }

        return groups;
    }
}
