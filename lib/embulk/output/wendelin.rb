Embulk::JavaPlugin.register_output(
  "wendelin", "org.embulk.output.wendelin.WendelinOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
