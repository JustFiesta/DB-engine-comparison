import os
import subprocess

def run_tests():
    tests_dir = "Tests"
    current_script = os.path.basename(__file__)
    test_scripts = sorted(
        [f for f in os.listdir(tests_dir) if f.endswith(".py") and f != current_script]
    )
    
    if not test_scripts:
        print("No test scripts found in the directory.")
        return

    for test_script in test_scripts:
        test_path = os.path.join(tests_dir, test_script)
        print(f"Running {test_script}...")
        
        try:
            result = subprocess.run(
                ["python3", test_path], 
                capture_output=True, 
                text=True, 
                check=True
            )
            print(f"Output of {test_script}:\n{result.stdout}")
        except subprocess.CalledProcessError as e:
            print(f"Error while running {test_script}:\n{e.stderr}")

if __name__ == "__main__":
    run_tests()
