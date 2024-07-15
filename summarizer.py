# from transformers import pipeline

# summarizer = pipeline("summarization", model="Falconsai/text_summarization")

# async def summary(article):
#     try:
#         summary = summarizer(article, max_length=1000, min_length=30, do_sample=False)
#         return {
#             "status": "success",
#             "summary": summary[0]['summary_text']
#         }
#     except Exception as e:
#         return {
#             "status": "error",
#             "message": str(e)
#         }