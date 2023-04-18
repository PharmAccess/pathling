library(pathlingR)

sparkR.session(master='local[*]', sparkJars=pathlingR.find_jar())
pathlingR.init()

json_resources_df <- read.text(system.file('data','ndjson', package='pathlingeR'))

print("json_resources_df")
print(head(json_resources_df))

condition_df <- encode_resource(json_resources_df, 'Condition')

print("condition_df")
print(head(condition_df))


