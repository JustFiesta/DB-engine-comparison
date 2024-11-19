import os
import sys
import importlib.util

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
        print(f"\nRunning {test_script}...")
        
        try:
            spec = importlib.util.spec_from_file_location("test_module", test_path)
            test_module = importlib.util.module_from_spec(spec)
            sys.modules["test_module"] = test_module
            spec.loader.exec_module(test_module) 
        except Exception as e:
            print(f"Error while running {test_script}:\n{e}")

if __name__ == "__main__":
    run_tests()
