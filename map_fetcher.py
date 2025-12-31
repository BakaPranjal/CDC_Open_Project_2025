import requests
import pandas as pd
import os
from typing import Tuple, Optional
import time
from pathlib import Path

def fetch_image(lat:float, lon:float, zoom:int = 16, size:str = "640x640") -> Optional[bytes]:

    ACCESS_TOKEN = "pk.eyJ1IjoiYmFrYS1wcmFuamFsIiwiYSI6ImNtamN2ZjRycjB4NWwzZXIwNG4xemNtZDIifQ.-VDq7PmWhiGlc3tGp40Vjw"
    url = (f"https://api.mapbox.com/styles/v1/mapbox/satellite-streets-v12/static/{lon},{lat},{zoom}/{size}")
    params = {
        "access_token": ACCESS_TOKEN
    }
    try:
        response = requests.get(url, params = params, timeout=10)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException as e :
        print(f"Error fetching image for ({lat}, {lon}): {e}")
        return None 

def image_Dataset(df: pd.DataFrame,lat_col: str = "lat", lon_col:str="long",timeout:float=0.1, output_dir:Path=Path("Map_Images_Test")) -> pd.DataFrame:

    imagePaths = []

    for idx,row in df.iterrows():
        lat = row[lat_col]
        lon = row[lon_col]

        filename = f"property_{idx}_{lat}_{lon}.png"
        filepath = output_dir / filename

        if filepath.exists():
            print(f"Image {idx+1}/{len(df)} already exists: {filename}")
            imagePaths.append(str(filepath))
            continue

        print(f"Fetching image {idx+1}/{len(df)}: {filename}")
        image_data = fetch_image(lat, lon)
        if image_data:
            with open(filepath, 'wb') as f:
                f.write(image_data)
            imagePaths.append(str(filepath))
        else:
            imagePaths.append(None)
        
        time.sleep(timeout)
    
    df['image_path'] = imagePaths
    return df



if __name__ == "__main__":
    # img = fetch_image(0,0)
    # if img:
    #     with open("map_test.png","wb") as f:
    #         f.write(img)
    
    output_dir = Path("Map_Images_Test")

    df = pd.read_csv('test2.csv')

    df_images = image_Dataset(df,output_dir=output_dir)
    df_images.to_csv("test_with_images.csv",index=False)
