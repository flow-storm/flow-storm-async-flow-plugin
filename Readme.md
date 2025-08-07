# FlowStorm core.async.flow plugin

![demo](./images/plugin_demo.png)

The core.async.flow plugin allows you to visualize your core.async.flow graph recorded activity from a graph view.

**Note: this plugin is still on alpha the same as core.async.flow!**
**This plugin is currently tested against core.async 1.9.808-alpha1**

**Requires FlowStorm >= 4.5.0**

# Setup

[![Clojars Project](https://img.shields.io/clojars/v/com.github.flow-storm/flow-storm-async-flow-plugin.svg)](https://clojars.org/com.github.flow-storm/flow-storm-async-flow-plugin)

In your deps.edn (same can be done with lein profiles) create an alias like :

```clojure
{...
 :aliases
 {...
  :fs-async-flow-plugin {:extra-deps {com.github.flow-storm/flow-storm-async-flow-plugin {:mvn/version "1.0.0-beta6"}} ;; check the latest
                         :jvm-opts ["-Dclojure.storm.instrumentOnlyPrefixes.asyncFlowPlugin=clojure.core.async.flow"
                                    "-Dflowstorm.plugins.namespaces.asyncFlowPlugin=flow-storm.plugins.async-flow.all"]}
}}}
```

Then, in your projects, just make sure you start your repl with the `:fs-async-flow-plugin` alias.

# Usage

The plugin needs you to record the total order timeline, so your recordings should be made with the second (from the right) recording button
enable.

In order to extract the graph structures from the recording it needs to record the flow start of your graph, basically when
`(flow/start graph)` gets called.

After that you can draw the graph and extract messages by going to the `Async Flow` plugin tab and selecting the flow-id, then clicking
the refresh button.
You can move around the nodes by dragging them to improve how the graph looks.

After the messages are loaded you should be able to **double-click** on a channel in the graph representation to see
the messages in the bottom panel.

**Important: the messages showing on each edge are the messages as received in the input channels of the receiving process,
not as they are leaving the sending process.**

If the message is arriving to a *non :compute node*, double clicking on it will take you to the code stepper right before that
message is going to be processed.



