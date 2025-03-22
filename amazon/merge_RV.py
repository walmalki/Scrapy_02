import json
import tkinter as tk
from tkinter import filedialog

def load_json_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return json.load(file)

def merge_json_files(file_paths):
    merged_data = []
    
    # Loop through each file and merge data
    for file_path in file_paths:
        data = load_json_file(file_path)
        print(f"📝 Loaded data from {file_path}")  # Print the first 5 entries for debugging
        merged_data.extend(data)
    
    return merged_data

def remove_duplicates(data, key, check_duplicates):
    if not check_duplicates:
        print("📝 Skipping duplicate removal...")
        return data
    
    seen = set()
    unique_data = []
    
    print(f"📝 Data before removing duplicates: {data[:0]}")  # Print the first 5 entries before removing duplicates
    for item in data:
        item_key = item.get(key)
        if item_key and item_key not in seen:
            unique_data.append(item)
            seen.add(item_key)
    
    print(f"📝 Data after removing duplicates: {unique_data[:0]}")  # Print the first 5 entries after removing duplicates
    return unique_data

def sort_data_by_serial_number(data, key):
    # Sort the data by extracting the numeric part of the serial number (e.g., AP1 -> 1, AP2 -> 2, ...)
    sorted_data = sorted(data, key=lambda x: int(x.get(key)[2:]))  # Skip 'AP' and convert to integer
    print(f"📝 Data after sorting by {key}: {sorted_data[:0]}")  # Print the first 5 entries after sorting
    return sorted_data

def save_json_file(data, output_path):
    with open(output_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)  # Ensure that non-ASCII characters are properly written

# Function to prompt user for file selection
def select_files():
    root = tk.Tk()
    root.withdraw()  # Hide the root window
    file_paths = filedialog.askopenfilenames(
        title="Select JSON Files", 
        filetypes=[("JSON files", "*.json")],
        initialdir="."  # Set initial directory to current directory
    )
    return list(file_paths)  # Return selected file paths as a list

# Example usage
if __name__ == "__main__":
    # Prompt user to select input files
    input_files = select_files()
    
    if not input_files:
        print("❌ No files selected. Exiting...")
    else:
        # Key to check for duplicates (it should be "serial_number" instead of "review_serial_number")
        key_to_check = 'AP_serial_number'
        
        # Ask user if they want to check for duplicates
        check_duplicates_input = input("Do you want to check for duplicates? (yes/no): ").strip().lower()
        check_duplicates = check_duplicates_input == 'yes'

        # Merge JSON files
        merged_data = merge_json_files(input_files)

        # Remove duplicates based on the specified key (if the switch is enabled)
        cleaned_data = remove_duplicates(merged_data, key_to_check, check_duplicates)

        # Sort the cleaned data by serial number to ensure order
        sorted_data = sort_data_by_serial_number(cleaned_data, key_to_check)

        # Save the cleaned, sorted data to a new file
        output_file = 'data/dataset_RV.json'
        save_json_file(sorted_data, output_file)

        print(f"📌 Data has been merged, cleaned, and saved to {output_file} without duplicates." if check_duplicates else 
              f"📌 Data has been merged, cleaned, and saved to {output_file} without removing duplicates.")
