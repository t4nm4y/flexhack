import google.generativeai as genai

GOOGLE_API_KEY="AIzaSyAxXjIMr8RtIriqwfGEedSbdGe4iWnXo8A"
genai.configure(api_key=GOOGLE_API_KEY)

def get_categories(html_content):
    print("\nAI is analysing...\n")
    model = genai.GenerativeModel('gemini-pro')
    prompt = ("""Below I have provided some content which contains the product names and other stuff present on a website, 
              go through it carefully and list out all category/types of products sold on the website. 
              if the products can be categorised just list the category name. Also tell the overall price-range(lowest, highest price found). 
              Strictly only give me a json like text(only) with 2 parameters: "categories", "price-range"\
              if any parameter value can't be found just return it as empty.\
    website_content: '{html_content}'\
    """).format(html_content=html_content)
    answer = model.generate_content(prompt)
    return answer.text