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
            var values = group.Values;
            
            // Instantiate transformer with collected values
            var transformer = factory.CreateFromConfiguration(values);
            if (transformer != null)
            {
                pipeline.Add(transformer);
            }
        }

        return pipeline;
    }

    private record Instruction(IDataTransformerFactory Factory, string Value);
    private record InstructionGroup(IDataTransformerFactory Factory, List<string> Values);

    private List<Instruction> ParseInstructions(string[] args)
    {
        var instructions = new List<Instruction>();
        
        // Map option aliases to factories
        // We need to know which CLI option corresponds to which factory
        // E.g. "--fake" -> FakeDataTransformerFactory
        var optionMap = new Dictionary<string, IDataTransformerFactory>(StringComparer.OrdinalIgnoreCase);
        
        foreach (var factory in _factories)
        {
            foreach (var option in factory.GetCliOptions())
            {
                if (!string.IsNullOrEmpty(option.Name))
                {
                    optionMap[option.Name] = factory;
                }
                foreach (var alias in option.Aliases)
                {
                    optionMap[alias] = factory;
                }
            }
        }

        for (int i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            
            // Is this a transformer option?
            if (optionMap.TryGetValue(arg, out var factory))
            {
                // Capture the value (next argument)
                if (i + 1 < args.Length)
                {
                    var value = args[i + 1];
                    instructions.Add(new Instruction(factory, value));
                    i++; // Skip next arg since we consumed it
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
                currentGroup = new InstructionGroup(instr.Factory, new List<string>());
                groups.Add(currentGroup);
            }
            
            currentGroup.Values.Add(instr.Value);
        }

        return groups;
    }
}
