import pandas as pd
import requests
import time
from GetAuthtoken import get_access_token


def post_data_to_api(api_url, token, input_excel, output_excel):
    df = pd.read_excel(input_excel)

    # Drop rows with missing/empty IDs
    df = df.dropna(subset=['farmer_id', 'user_id'])
    df = df[(df['farmer_id'].astype(str).str.strip() != '') & (df['user_id'].astype(str).str.strip() != '')]

    # Convert to string (ensure no decimals etc.)
    df['farmer_id'] = df['farmer_id'].astype(int).astype(str)
    df['user_id'] = df['user_id'].astype(int).astype(str)

    # Add necessary columns if not present
    for col in ['Status', 'Processed Farmer IDs', 'Processed User IDs', 'API Response']:
        if col not in df.columns:
            df[col] = ''

    headers = {
        "Authorization": token
    }

    batch_size = 100
    total = len(df)
    for i in range(0, total, batch_size):
        chunk_df = df.iloc[i:i + batch_size]
        farmer_ids = chunk_df['farmer_id'].tolist()
        user_ids = chunk_df['user_id'].unique().tolist()

        farmers_param = ','.join(farmer_ids)
        users_param = ','.join(user_ids)

        print(f"\n🔁 Iteration {i // batch_size + 1}")
        print(f"📦 Farmers: {farmers_param}")
        print(f"👥 Users: {users_param}")

        response_text = ""
        try:
            response = requests.post(
                f"{api_url}?farmers={farmers_param}&users={users_param}",
                headers=headers
            )
            response_text = response.text

            if response.status_code == 200:
                status = 'Success'
            else:
                status = f"Failed: {response.status_code}"

        except Exception as e:
            status = f"Error: {e}"
            response_text = str(e)

        # Update each row in the chunk
        df.loc[chunk_df.index, 'Status'] = status
        df.loc[chunk_df.index[0], 'Processed Farmer IDs'] = farmers_param
        df.loc[chunk_df.index[0], 'Processed User IDs'] = users_param
        df.loc[chunk_df.index[0], 'API Response'] = response_text

        df.to_excel(output_excel, index=False)
        time.sleep(1)

    print("\n✅ All chunks processed. Output saved to:", output_excel)


if __name__ == "__main__":
    input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\farmeridsanduserids.xlsx"
    output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\processed_farmerids_userids_with_response.xlsx"
    api_url = "https://us-v2-gcp.cropin.co.in/qa8/services/farm/api/farmers/assign-multiple-users"
    environment = "prod1"

    print("⏳ Retrieving access token...")
    token = get_access_token("asp", "9649964096", "123456", environment)

    if token:
        print("✅ Token acquired!")
        post_data_to_api(api_url, token, input_excel, output_excel)
    else:
        print("❌ Failed to get token. Exiting.")
