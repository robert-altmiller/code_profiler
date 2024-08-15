import json, subprocess

def run_scalene_profiling(method = 'json', script_path = None, scalene_output_path = "./scalene"):
    """
    run scalene profiling
    method = "json" or "html"
    """
    # Running Scalene with the --method flag
    output = subprocess.run(['scalene', f'--{method}', script_path], capture_output=True, text=True)

    if output.stdout:
        if method == 'json':
            # Parse and save the JSON output
            profile_data = json.loads(output.stdout)
            with open(f"{scalene_output_path}.{method}", 'w') as json_file:
                json_file.write(json.dumps(profile_data, indent=4))

        if method == 'html':
            # Save the HTML output to a file
            with open(f"{scalene_output_path}.{method}", 'w') as html_file:
                html_file.write(output.stdout)
    else:
        print("No output from Scalene or error occurred:", output.stderr)

# Run scalene profiling as json
run_scalene_profiling(
    method = "json",
    script_path = "code_example.py"
)

# Run scalene profiling as html
run_scalene_profiling(
    method = "html",
    script_path = "code_example.py"
)