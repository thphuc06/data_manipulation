import base64
from io import BytesIO
from PIL import Image

def convert_image_to_jpg(input_string, output_path):
    data = input_string
    data = data[data.index(',')+1:]
    
    bytes_decoded = base64.b64decode(data)
    image = Image.open(BytesIO(bytes_decoded))

    out_jpg = image.convert("RGB")
    out_jpg.save(output_path, "JPEG")
    
    # Rất quan trọng: Phải đóng file để giải phóng bộ nhớ
    image.close()
    out_jpg.close()
    
    return True