-- Recommended dbt-lsp setup for Neovim
-- Save this file or copy its contents to your init.lua / plugins config

-- 1. Define the custom server config
local configs = require('lspconfig.configs')
local lspconfig = require('lspconfig')
local util = require('lspconfig.util')

if not configs.dbt_lsp then
  configs.dbt_lsp = {
    default_config = {
      cmd = { "/Users/Ahrne/dbt-lsp/build_output/release/dbt-lsp" },
      filetypes = { 'sql', 'mysql', 'postgres' }, -- Add 'sql' to trigger
      root_dir = util.root_pattern("dbt_project.yml"),
      settings = {},
    },
  }
end

-- 2. Setup the server
lspconfig.dbt_lsp.setup {
  on_attach = function(client, bufnr)
    print("dbt-lsp attached!")
    -- Mappings, etc.
  end,
}
