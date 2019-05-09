
# MobiusCore: C# API for Spark

[MobiusCore](https://github.com/cite-sa/MobiusCore) is a .NET Core port of [Mobius](https://github.com/microsoft/Mobius), the open source implementation of C# Apache Spark bindings. 

# Implementation

[Mobius](https://github.com/microsoft/Mobius) implementation relies heavily on Delegate serialization, a feature that was dropped from .NET Core (view discussion [here](https://github.com/dotnet/corefx/issues/19119) ). As a result this rendered it impossible for Mobius to support the .NET Core platform. 

[MobiusCore](https://github.com/cite-sa/MobiusCore) replaces all the Delegate Function parameters in  [Mobius](https://github.com/microsoft/Mobius) Functions
with equivalent Lambda Function in the form of Linq Expressions, in order to tackle the serialization problem that was introduced in .NET Core with Delegate Serialization. 

In example

```c#
public class Program
{
    public String Map(Func<String, String, String> func)
    {
        var value1 = "Func";
        var value2 = "Demonstration";
        return func(value1, value2);
    }

    public String Concatenate(String first, String second)
    {
        return String.Join(" ", first, second);
    }

    public static void Main(string[] args)
    {
        var program = new Program();
        var output = program.Map(program.Concatenate);
        Console.WriteLine(output);
    }
}
```

is equivalent to 

```c#
public class Program
{
    public String Map(Func<String, String, String> func)
    {
        var value1 = "Func";
        var value2 = "Demonstration";
        return func(value1, value2);
    }

    public String Concatenate(String first, String second)
    {
        return String.Join(" ", first, second);
    }

    public static void Main(string[] args)
    {
        var program = new Program();
        var output = program.Map((x, y) => new Program().Concatenate(x, y));
        Console.WriteLine(output);
    }
}
```

which is also equivalent to

```c#
public class Program
{
    public String Map(Expression<Func<String, String, String>> expression)
    {
        var value1 = "Func";
        var value2 = "Demonstration";
        var func = expression.Compile();
        return func(value1, value2);
    }

    public String Concatenate(String first, String second)
    {
        return String.Join(" ", first, second);
    }

    public static void Main(string[] args)
    {
        var program = new Program();
        var output = program.Map((x, y) => new Program().Concatenate(x, y));
        Console.WriteLine(output);
    }
}
```

As a result Linq Expressions can be used to replace Delegates. 

Linq Expressions represent a strongly typed lambda expression as an expression tree. As a result extracted data from the expression tree can be serialized. For this purpose we are a using [Serialize.Linq](https://github.com/esskar/Serialize.Linq). We are serializing the Expression, deserialize it on the Worker end,  reconstruct and execute it.

# Drowbacks from this approach

By using Linq Expressions instead of Delegates the following language features cannot be used.

- Statement body when using Mobius Core functions. You will need to create a Serializable Helper Class with the required function if you need something like that.
- Dynamically bound expressions are not allowed
- Functions with optional parameters are not allowed
