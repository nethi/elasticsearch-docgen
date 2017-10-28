# elasticsearch-docgen
Simple tool to generate documents (based on jinja templates) into different indices


usage:  docgen.py <profile diretory> init|run|reset|stats|gendoc
  
  profile directory = directory containing profile.json and other index and data templates
  
  init = one time initialization of indices defined in the profile
  
  run =  add documents based on the mappings and templates. Args: Doc count
  
  reset = delete indices specified in the profile
  
  stats = Dump statistics
  
  gendoc = Test: Generate one doc per index and get out

