Embulk::JavaPlugin.register_input(
  "salesforce_bulk", "org.embulk.input.salesforce_bulk.SalesforceBulkInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
