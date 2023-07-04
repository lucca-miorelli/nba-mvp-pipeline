from prefect.filesystems import GitHub

block = GitHub(
    repository="https://github.com/lucca-miorelli/nba-mvp-pipeline/",
    reference="prefect-pipeline",
    subfolder="src/prefect_pipeline.py",
)
# block.get_directory("") # specify a subfolder of repo
block.save("test-gh", overwrite=True)
