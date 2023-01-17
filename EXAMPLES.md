# Examples

First, be sure to follow the [quickstart guide](QUICKSTART.md) to setup the system.

## Single task with no inputs

First, take a look at the [job definition file](examples/single-task-no-input/single-task.yaml)
for a single task with no inputs.  This is a [YAML](https://yaml.org/) file which completely
specifies the task for the PanDA system.  This example contains three blocks: `Parameters`,
`OutputDataSets` and `JobCommands`.

### Parameters

The `Parameters` block provides 

```
$ edit examples/single-task-no-input/single-task.yaml
$ shrek --submit --tag STNI examples/single-task-no-input/single-task.yaml
```



