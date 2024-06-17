# Recommendations for improving the Whisp codebase quality and readability

## 1. Add type hints and docs for every new function/method written

**TL;DR: Understanding what a function does  is so much easier when type hints and a docstring are available. This is true for newcomers, but also contributors re-discovering their own code after a while**

Type hints allow the reader to understand what kind of arguments a function expects and therefore how it uses its arguments and what kind of value it returns. It also allows coding platforms to automatically analyze the code and detect inconsistencies within the codebase.

It may take time to type hint and document the entire codebase. A good way to get started is to systematically add type hints and a docstring whenever a new function is written, or whenever and existing one is modified. Keep in mind that the best time to document a function is when you just finished writing it, as you still have in mind what each variable is!

Documenting the functions/methods is also crucial in uderstanding them and allowing newcomers to quickly understand the codebase without requiring lots of assistance and walkthrough.

Each function must have its docstring (delimited by three double quotes), ideally following this structure:

```
def some_function(
    arg1: type1,
    arg2: type2,
    arg3: type3 = default_value
) -> return_type:

    """One short sentence explanation of what the function does.

    (optional) A longer explanation with details if needed.

    Parameters
    ----------
    arg1 : type1
        Short sentence explaining what is arg1.
    arg2 : type2
        Short sentence explaining what is arg2.
    arg3 : type3, default: default_value
        Short sentence explaining what is arg3.

    Returns
    -------
    return_value : return_type
        Short sentence explaining what is the returned value.
    """

    (here starts function code)
```

In this example we follow the Numpy docs style, but other style exist that are widespread among the Python community. For consistency purposes, all docs should follow the same style conventions. This information should therefore be agreed upon by the development leading team and specified in the contributor's guide.


## 2. Imports must be transparent

Putting all imports into one module and then running `from my_big_imports_module import *` renders imports and dependencies completely untraceable. This has notorious consequences:
- Readers cannot understand what dependencies a specific block requires in order to function
- Automatic editor tools cannot build the dependency tree and cannot help contributors to detect missing/conflicting dependencies
- Contributors cannot determine whether a dependency is still required without scanning the entire codebase

Python usually does a great job at handling duplicate imports from different modules at runtime. It is therefore greatly recommended to explicitely write the import statement of a given package inside every module that requires it.


## 3. Notebooks are for demonstration purposes only

Notebooks are great for experimenting and easily visualizing the intermediate states of sequential execution of blocks of code. But they lack the modularity and integration capabilities of regular modules. They also cannot be easily processed by code analysis/linting tools. Therefore they should not be used to store functionalities that may be required outside of them. Such functionalities belong to regular modules.

On the other hands, notebooks are great demonstration platforms for new users to understand how to use your code and display examples of what it can do for them. Keep in mind that those notebooks must be maintained, i.e. tested to see if they still work once you made code modifications. You don't want your potential new users to feel disappointed and move on when they fail to run your demo!


## 4. Names must be explicit

While in the docstring example we named our first argument `arg1`, this is actually poor practice in real code as it is a lost opportunity to bring clarity to the code. Whether it is a module, class, function, argument or even a function-scoped, intermediate-calculation varaible, always try to give it an explicit, concise name. That will improve readability for reviewers and contributors. And try to follow Python's naming conventions!


## 5. Configs

Try to group config information in a small number of files, in order to avoid having to wade through numerous files to find one parameter, and limit the number of required imports.

If some config parameters are intended to be user-customizable, do not include them in the codebase! Once the codebase is packaged, it would be very inconvenient if the user had to re-install the package every time they update a single config field!

For small numbers of config fields, one can use argument parsers so that when a user runs a script, they can add the required arguments to the command line.

Another option for when a script requires a lot of user-defined config is to expect a dedicated config file that the user must fill before running the script (common formats for these are YAML or JSON). In that case, it is good practice to include a template of such a config file in the codebase with dummy or reasonable values, in order to guide the user into how to fill the config.
