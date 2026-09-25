// Prompts that name a specific MCP server's tools. A generic "use your tools"
// mostly gets "I need more context", or Claude Code's built-in tools instead.
export const TOOL_PROMPTS = {
  oracle: ['Ask the oracle whether we should deploy on Friday, then roll 1d20 to pick the chaos target. Report both.',
           'Roll 2d20 with your dice tool and ask the oracle if today is safe for a chaos experiment. One sentence each.'],
  coffee: ['Brew an incident-size espresso with the coffee machine, then check the bean level. Report both.',
           'Check the bean level and tell me the on-call morale index, then brew a large flat white.'],
  excuses: ['Checkout is down: generate an excuse for checkout, then check whether it is DNS. Report both.',
            'Run the DNS blame check for "5xx on login", then generate an excuse for the login service.'],
};
