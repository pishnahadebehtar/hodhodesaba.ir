import os
import json
import time
import re
import random
from datetime import datetime
import html
from urllib.parse import urlparse

try:
    from feedparser import parse as parse_rss
    from bs4 import BeautifulSoup
    import requests
except ImportError as e:
    raise ImportError(f"Failed to import required packages: {str(e)}. Ensure feedparser, beautifulsoup4, requests, and appwrite are installed.")

from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query

def truncate_text(text, max_chars=2000):
    if not text or len(text) <= max_chars:
        return text or ""
    truncated = text[:max_chars]
    last_period = truncated.rfind('.')
    if last_period > 0:
        return truncated[:last_period + 1]
    return truncated[:max_chars].rsplit(' ', 1)[0] + '...' if truncated else text[:max_chars]

def shorten_url(url, max_length=250):
    if len(url) <= max_length:
        return url
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    if len(base) <= max_length:
        return base
    return base[:max_length-3] + '...'

def scrape_article_text(url, context):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    context.log(f"Scraping URL {url}")
    try:
        response = requests.get(url, headers=headers, timeout=5)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        content = ''
        for tag in soup.find_all(['p', 'article', 'div']):
            text = tag.get_text(strip=True)
            if text and len(text) > 50:
                content += text + ' '
        content = re.sub(r'\s+', ' ', content).strip()
        if not content:
            context.log(f"No meaningful content found at {url}")
            return "No content available"
        return content
    except Exception as e:
        context.log(f"Scraping failed for {url}: {str(e)}")
        return "Scraping failed"

def partial_parse_json(response, context):
    try:
        cleaned_response = response.strip().strip('"\'')
        data = json.loads(cleaned_response)
        return data
    except json.JSONDecodeError as e:
        context.log(f"Full JSON parsing failed: {str(e)}. Attempting partial parsing.")
        try:
            title_match = re.search(r'"title"\s*:\s*"([^"]+)"', response)
            summary_match = re.search(r'"summary"\s*:\s*"([^"]+)"', response)
            explanation_match = re.search(r'"full_explanation"\s*:\s*"([^"]+)"', response)
            category_match = re.search(r'"category"\s*:\s*"([^"]+)"', response)
            tags_match = re.search(r'"tags"\s*:\s*\[([^\]]*)\]', response)

            result = {}
            if title_match:
                result['title'] = title_match.group(1)[:255]
            if summary_match:
                result['summary'] = summary_match.group(1)[:100]
            if explanation_match:
                result['full_explanation'] = explanation_match.group(1)
            if category_match:
                result['category'] = category_match.group(1)
            if tags_match:
                tags = [tag.strip().strip('"') for tag in tags_match.group(1).split(',') if tag.strip()]
                result['tags'] = tags if 3 <= len(tags) <= 5 else None

            if result:
                context.log(f"Partially parsed JSON: {json.dumps(result, ensure_ascii=False)}")
                return result
        except Exception as e:
            context.log(f"Partial JSON parsing failed: {str(e)}")
    return None

def refine_article_with_ai(original_title, original_summary, full_explanation, feed_name, context):
    # Enhanced prompt with JSON format specification and example
    prompt = (
        f"You are processing a news article from {feed_name}. "
        f"Based on the provided title, summary, and scraped content, perform the following: "
        f"1. Translate the title (if in English) or regenerate it (if in Persian) to a concise, accurate Persian title (max 255 characters). "
        f"2. Translate the summary (if in English) or regenerate it (if in Persian) to a concise Persian summary (max 100 characters). "
        f"3. Summarize the scraped content to produce a complete and coherent Persian explanation relevant to the title and summary. "
        f"   - The summary must be 1500–2000 characters long, unless the content is insufficient, then use all relevant content. "
        f"   - Ensure the summary ends naturally, not mid-sentence, and covers key details without omitting critical information. "
        f"   - Remove irrelevant parts (e.g., advertisements, navigation menus) and translate to Persian if necessary. "
        f"4. Assign a category in Persian from this list: سیاست, اقتصاد, فناوری, سلامت, ورزش, سرگرمی, جهان, based on the content. "
        f"5. Generate 3-5 relevant Persian tags (e.g., 'هسته‌ای', 'اقتصاد جهانی') based on the content. "
        f"Return a valid JSON object with keys: title (string), summary (string), full_explanation (string), category (string), tags (array of strings). "
        f"The response must be properly formatted JSON, enclosed in {{}}. If the JSON is malformed or missing required keys, the program will throw an error and skip the article. "
        f"Example JSON format:\n"
        f'{{\n'
        f'  "title": "وزیر آفریقای جنوبی اتهامات بی‌اساس را رد کرد",\n'
        f'  "summary": "وزیر پلیس ادعاهای نادرست را تکذیب کرد.",\n'
        f'  "full_explanation": "وزیر پلیس آفریقای جنوبی اظهارات مطرح شده درباره وقایع اخیر را نادرست خواند و اطلاعات دقیقی ارائه کرد... (1500–2000 characters)",\n'
        f'  "category": "جهان",\n'
        f'  "tags": ["آفریقای جنوبی", "سیاست", "خبر بین‌المللی", "وزیر پلیس"]\n'
        f'}}'
        f"\n\nOriginal Title: {original_title}\n"
        f"Original Summary: {original_summary}\n"
        f"Scraped Content: {full_explanation}\n\n"
        f"Output only the JSON object, no additional text."
    )

    # OpenRouter API keys
    openrouter_api_keys = [
       
    ]

    # Try OpenRouter with each API key
    for idx, api_key in enumerate(openrouter_api_keys, 1):
        openrouter_url = "https://openrouter.ai/api/v1/chat/completions"
        openrouter_headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        openrouter_payload = {
            "model": "meta-llama/llama-4-maverick:free",
            "messages": [{"role": "user", "content": prompt}]
        }

        context.log(f"Calling OpenRouter API (Attempt {idx}) for article: {original_title}")
        start_time = time.time()
        try:
            response = requests.post(openrouter_url, headers=openrouter_headers, json=openrouter_payload, timeout=5)
            response.raise_for_status()
            elapsed_time = time.time() - start_time
            context.log(f"OpenRouter (Attempt {idx}) response time: {elapsed_time:.2f} seconds")
            result = response.json()
            ai_response = result.get('choices', [{}])[0].get('message', {}).get('content', '{}')
            context.log(f"OpenRouter (Attempt {idx}) raw response (first 500 chars): {ai_response[:500]}")
            ai_response = ai_response.strip().encode('utf-8').decode('utf-8')
            try:
                refined_data = json.loads(ai_response)
                tags = refined_data.get('tags', ["خبر", "جهان", feed_name.lower().replace(" ", "_")])
                if not isinstance(tags, list) or len(tags) < 3 or len(tags) > 5:
                    tags = ["خبر", "جهان", feed_name.lower().replace(" ", "_")]
                full_explanation = refined_data.get('full_explanation', '')
                if not full_explanation or len(full_explanation) < 500:
                    context.log(f"OpenRouter (Attempt {idx}) full_explanation invalid or too short ({len(full_explanation)} chars). Trying next API.")
                    continue
                return {
                    "title": refined_data.get('title', original_title)[:255],
                    "summary": refined_data.get('summary', original_summary)[:100],
                    "full_explanation": full_explanation,
                    "category": refined_data.get('category', "جهان"),
                    "tags": tags
                }
            except json.JSONDecodeError:
                refined_data = partial_parse_json(ai_response, context)
                if refined_data:
                    tags = refined_data.get('tags', ["خبر", "جهان", feed_name.lower().replace(" ", "_")])
                    if not tags or len(tags) < 3 or len(tags) > 5:
                        tags = ["خبر", "جهان", feed_name.lower().replace(" ", "_")]
                    full_explanation = refined_data.get('full_explanation', '')
                    if not full_explanation or len(full_explanation) < 500:
                        context.log(f"OpenRouter (Attempt {idx}) full_explanation invalid or too short ({len(full_explanation)} chars). Trying next API.")
                        continue
                    return {
                        "title": refined_data.get('title', original_title)[:255],
                        "summary": refined_data.get('summary', original_summary)[:100],
                        "full_explanation": full_explanation,
                        "category": refined_data.get('category', "جهان"),
                        "tags": tags
                    }
                context.log(f"OpenRouter (Attempt {idx}) JSON parsing failed. Trying next API.")
                continue
        except Exception as e:
            elapsed_time = time.time() - start_time
            context.log(f"OpenRouter (Attempt {idx}) API call failed for '{original_title}': {str(e)}. Response time: {elapsed_time:.2f} seconds. Trying next API.")
            continue

    # Final attempt: Aval AI
    avalai_api_key = os.environ.get('AVALAI_API_KEY')
    if not avalai_api_key:
        context.log("AVALAI_API_KEY not found. Skipping article.")
        return None

    avalai_url = "https://api.avalai.ir/v1/chat/completions"
    avalai_headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {avalai_api_key}"
    }
    avalai_payload = {
        "model": "gpt-4o-mini",
        "messages": [{"role": "user", "content": prompt}]
    }

    context.log(f"Calling Aval AI API (Final attempt) for article: {original_title}")
    start_time = time.time()
    try:
        response = requests.post(avalai_url, headers=avalai_headers, json=avalai_payload, timeout=10)
        response.raise_for_status()
        elapsed_time = time.time() - start_time
        context.log(f"Aval AI response time: {elapsed_time:.2f} seconds")
        result = response.json()
        ai_response = result.get('choices', [{}])[0].get('message', {}).get('content', '{}')
        context.log(f"Aval AI raw response (first 500 chars): {ai_response[:500]}")
        ai_response = ai_response.strip().encode('utf-8').decode('utf-8')
        try:
            refined_data = json.loads(ai_response)
            tags = refined_data.get('tags', ["خبر", "جهان", feed_name.lower().replace(" ", "_")])
            if not isinstance(tags, list) or len(tags) < 3 or len(tags) > 5:
                tags = ["خبر", "جهان", feed_name.lower().replace(" ", "_")]
            full_explanation = refined_data.get('full_explanation', '')
            if not full_explanation or len(full_explanation) < 500:
                context.log(f"Aval AI full_explanation invalid or too short ({len(full_explanation)} chars). Skipping article.")
                return None
            return {
                "title": refined_data.get('title', original_title)[:255],
                "summary": refined_data.get('summary', original_summary)[:100],
                "full_explanation": full_explanation,
                "category": refined_data.get('category', "جهان"),
                "tags": tags
            }
        except json.JSONDecodeError:
            refined_data = partial_parse_json(ai_response, context)
            if refined_data:
                tags = refined_data.get('tags', ["خبر", "جهان", feed_name.lower().replace(" ", "_")])
                if not tags or len(tags) < 3 or len(tags) > 5:
                    tags = ["خبر", "جهان", feed_name.lower().replace(" ", "_")]
                full_explanation = refined_data.get('full_explanation', '')
                if not full_explanation or len(full_explanation) < 500:
                    context.log(f"Aval AI full_explanation invalid or too short ({len(full_explanation)} chars). Skipping article.")
                    return None
                return {
                    "title": refined_data.get('title', original_title)[:255],
                    "summary": refined_data.get('summary', original_summary)[:100],
                    "full_explanation": full_explanation,
                    "category": refined_data.get('category', "جهان"),
                    "tags": tags
                }
            context.log("Aval AI JSON parsing failed. Skipping article.")
            return None
    except Exception as e:
        elapsed_time = time.time() - start_time
        context.log(f"Aval AI API call failed for '{original_title}': {str(e)}. Response time: {elapsed_time:.2f} seconds. Skipping article.")
        return None

def fetch_rss_feed(task, context, start_time):
    if time.time() - start_time > 550:
        context.log(f"Approaching 600-second timeout. Skipping task: {task['name']}")
        return None

    rss_url = task["url"]
    feed_name = task["name"]
    context.log(f"Fetching RSS feed for {feed_name}")
    try:
        feed_data = parse_rss(rss_url)
        if not hasattr(feed_data, 'entries') or not feed_data.entries:
            context.log(f"Invalid or empty RSS feed for {feed_name}: No entries found")
            if feed_data and hasattr(feed_data, 'bozo_exception'):
                context.log(f"RSS parsing error: {str(data.bozo_exception)}")
            return None
        latest_entry = feed_data.entries[0]
        article_url = latest_entry.get('link', '')
        if not article_url:
            context.log(f"No valid URL found for article in {feed_name}")
            return None
        full_explanation = scrape_article_text(article_url, context)
        if full_explanation == "Scraping failed":
            context.log(f"Scraping failed for {article_url}. Using RSS summary as fallback.")
            full_explanation = html.unescape(re.sub(r'<[^>]+>', '', latest_entry.get('description', latest_entry.get('summary', 'No content available'))))
        original_title = latest_entry.get('title', 'Unknown Title')
        original_summary = truncate_text(html.unescape(re.sub(r'<[^>]+>', '', latest_entry.get('description', latest_entry.get('summary', '')))), 100)
        refined_data = refine_article_with_ai(original_title, original_summary, full_explanation, feed_name, context)
        if not refined_data:
            context.log(f"AI processing failed for {feed_name}. Skipping article.")
            return None
        context.log(f"Found article for {feed_name}: {refined_data['title']}")
        return {
            "title": refined_data["title"],
            "summary": refined_data["summary"],
            "full_explanation": refined_data["full_explanation"],
            "citations": [shorten_url(article_url)],
            "category": refined_data["category"],
            "tags": refined_data["tags"],
            "source": feed_name,
            "task_id": task["$id"]
        }
    except Exception as e:
        context.log(f"RSS fetch failed for {feed_name}: {str(e)}")
        return None

def process_rss_feeds(context, databases, start_time):
    results = []
    valid_categories = ['سیاست', 'اقتصاد', 'فناوری', 'سلامت', 'ورزش', 'سرگرمی', 'جهان']

    context.log("Fetching tasks with isdone: false")
    try:
        tasks_response = databases.list_documents(
            database_id=os.environ['APPWRITE_DATABASE_ID'],
            collection_id=os.environ['APPWRITE_SCRAPE_TASKS_COLLECTION_ID'],
            queries=[Query.equal("isdone", False)]
        )
        tasks = tasks_response['documents']
        context.log(f"Found {len(tasks)} tasks with isdone: false")
    except Exception as e:
        context.log(f"Failed to fetch tasks: {str(e)}")
        return results

    if len(tasks) == 0:
        context.log("No tasks with isdone: false found")
        return results

    selected_tasks = random.sample(tasks, min(2, len(tasks)))
    context.log(f"Selected {len(selected_tasks)} tasks: {[task['name'] for task in selected_tasks]}")

    for task in selected_tasks:
        if time.time() - start_time > 550:
            context.log(f"Approaching 600-second timeout. Stopping task processing.")
            break

        elapsed_time = time.time() - start_time
        context.log(f"Processing task: {task['name']} (Elapsed time: {elapsed_time:.2f} seconds)")

        article = fetch_rss_feed(task, context, start_time)
        if not article:
            context.log(f"Skipping task {task['name']}: No valid article retrieved")
            continue

        title = article['title']
        source = article['source']
        task_id = article['task_id']

        try:
            context.log(f"Checking for duplicates: {title}")
            existing = databases.list_documents(
                database_id=os.environ['APPWRITE_DATABASE_ID'],
                collection_id=os.environ['APPWRITE_NEWS_ARTICLES_COLLECTION_ID'],
                queries=[Query.equal("title", title), Query.equal("date", datetime.utcnow().strftime('%Y-%m-%d'))]
            )
            if existing['total'] > 0:
                context.log(f"Skipping duplicate article from {source}: {title}")
                continue
        except Exception as e:
            context.log(f"Failed to check duplicates for '{title}' from {source}: {str(e)}")

        required_keys = ['title', 'summary', 'full_explanation', 'citations', 'category', 'tags']
        if not all(key in article for key in required_keys):
            context.log(f"Invalid article data from {source}: {json.dumps(article)}")
            continue
        if article['category'] not in valid_categories:
            context.log(f"Invalid category for '{title}' from {source}: {article['category']}. Setting to 'جهان'")
            article['category'] = 'جهان'
        if len(article['summary']) > 100:
            article['summary'] = article['summary'][:100]
            context.log(f"Truncated summary to 100 chars for '{title}' from {source}")
        if len(article['full_explanation']) > 2000:
            context.log(f"full_explanation too long ({len(article['full_explanation'])} chars) for '{title}' from {source}. Truncating gracefully.")
            article['full_explanation'] = truncate_text(article['full_explanation'], 2000)

        doc = {
            'title': article['title'],
            'summary': article['summary'],
            'full_explanation': article['full_explanation'],
            'citations': article['citations'],
            'date': datetime.utcnow().strftime('%Y-%m-%d'),
            'source': source,
            'tags': article['tags'],
            'category': article['category']
        }

        try:
            context.log(f"Storing article: {title}")
            databases.create_document(
                database_id=os.environ['APPWRITE_DATABASE_ID'],
                collection_id=os.environ['APPWRITE_NEWS_ARTICLES_COLLECTION_ID'],
                document_id='unique()',
                data=doc
            )
            context.log(f"Stored article: {title} from {source}")
        except Exception as e:
            context.log(f"Failed to store article '{title}' from {source}: {str(e)}")
            continue

        try:
            context.log(f"Updating task {task['name']} (ID: {task_id}) isdone to true")
            databases.update_document(
                database_id=os.environ['APPWRITE_DATABASE_ID'],
                collection_id=os.environ['APPWRITE_SCRAPE_TASKS_COLLECTION_ID'],
                document_id=task_id,
                data={"isdone": True}
            )
            context.log(f"Updated task {task['name']} isdone to true")
        except Exception as e:
            context.log(f"Failed to update task '{task['name']}' isdone: {str(e)}")
            continue

        results.append(article)
        context.log(f"Processed article from {source}: {json.dumps(article, ensure_ascii=False)}")

    try:
        context.log("Checking if all tasks are isdone: true")
        all_tasks_response = databases.list_documents(
            database_id=os.environ['APPWRITE_DATABASE_ID'],
            collection_id=os.environ['APPWRITE_SCRAPE_TASKS_COLLECTION_ID'],
            queries=[Query.equal("isdone", False)]
        )
        if all_tasks_response['total'] == 0:
            context.log("All tasks are isdone: true. Resetting all to isdone: false")
            all_tasks = databases.list_documents(
                database_id=os.environ['APPWRITE_DATABASE_ID'],
                collection_id=os.environ['APPWRITE_SCRAPE_TASKS_COLLECTION_ID']
            )['documents']
            for task in all_tasks:
                try:
                    databases.update_document(
                        database_id=os.environ['APPWRITE_DATABASE_ID'],
                        collection_id=os.environ['APPWRITE_SCRAPE_TASKS_COLLECTION_ID'],
                        document_id=task['$id'],
                        data={"isdone": False}
                    )
                    context.log(f"Reset task {task['name']} to isdone: false")
                except Exception as e:
                    context.log(f"Failed to reset task '{task['name']}' isdone: {str(e)}")
    except Exception as e:
        context.log(f"Failed to check or reset tasks: {str(e)}")

    return results

def main(context):
    req = context.req
    res = context.res
    start_time = time.time()

    try:
        context.log(f"Function execution started at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
        context.log("Initializing Appwrite client")
        client = Client()
        client.set_endpoint(os.environ['APPWRITE_ENDPOINT'])
        client.set_project(os.environ['APPWRITE_PROJECT_ID'])
        client.set_key(os.environ['APPWRITE_API_KEY'])
        databases = Databases(client)

        context.log("Verifying dependencies: feedparser, beautifulsoup4, requests, appwrite")
        try:
            import feedparser
            import bs4
            import requests
            import appwrite
            context.log("Dependencies loaded successfully")
        except ImportError as e:
            context.log(f"Dependency import failed: {str(e)}")
            raise ImportError(f"Failed to import dependencies: {str(e)}")

        context.log("Starting RSS task processing")
        results = process_rss_feeds(context, databases, start_time)
        elapsed_time = time.time() - start_time
        context.log(f"Processing completed successfully with {len(results)} articles in {elapsed_time:.2f} seconds")
        return res.json({'message': 'Processing completed successfully', 'articles': len(results)})

    except Exception as e:
        elapsed_time = time.time() - start_time
        context.log(f"General error after {elapsed_time:.2f} seconds: {str(e)}")
        return res.json({'error': str(e)})