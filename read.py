def calculate_average_tpm(file_path):
    total_tpm = 0
    count = 0
    
    with open(file_path, 'r') as file:
        for line in file:
            parts = line.split(',')
            progress = float(parts[0].split(':')[1].strip())
            
            if progress > 120:
                for part in parts:
                    if 'tpmTOTAL' in part:
                        # Extract the numerical value of tpmTOTAL
                        value = float(part.split(':')[1].strip())
                        total_tpm += value
                        count += 1

    # Calculate the average of tpmTOTAL values for lines with progress > 120
    average_tpm = total_tpm / count if count != 0 else 0
    return average_tpm

# file_path = 'tps-ori-1.txt'
file_path = 'tps-rev1-1.txt'
# file_path = 'tps-rev1-2.txt'
print("Average tpmTOTAL for progress > 120:", calculate_average_tpm(file_path))