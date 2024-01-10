import requests
def check_status(url):
    try:
        response = requests.get(url, allow_redirects=True, timeout=5)
        return response.status_code == 200
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return False
    
print(check_status('https://www.bata.in'))