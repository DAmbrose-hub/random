import pandas as pd
import socket

def resolve_ip(fqdn):
    try:
        ip_address = socket.gethostbyname(fqdn)
        return ip_address
    except socket.gaierror:
        return None

def main():
    # Read the Excel file and the "nonprod" sheet
    excel_filename = 'input_data.xlsx'
    df = pd.read_excel(excel_filename, sheet_name='nonprod')

    # Ensure that the sheet contains 'hostname' and 'fqdn' columns
    if 'hostname' not in df.columns or 'fqdn' not in df.columns:
        raise ValueError("The 'nonprod' sheet should contain 'hostname' and 'fqdn' columns.")

    # Resolve IP addresses and add the "ip" column
    df['ip'] = df['fqdn'].apply(resolve_ip)

    # Save the updated DataFrame with IP addresses to a new Excel file
    output_excel_filename = 'output_data.xlsx'
    df.to_excel(output_excel_filename, index=False)

    print(f"Output saved to {output_excel_filename}")

if __name__ == "__main__":
    main()
