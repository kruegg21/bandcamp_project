Pipeline:  
1. Scrape data from www.bandcamp.com using bandcamp_scraper.py and dumps the data to MongoDB  
2. Data is converted from JSON form to Graphlab SFrame form using main.py  
3. Best model is trained using item_similarity_model.py  
4. App is deployed using app.py on an Amazon EC2 instance 
  
YouTube video of fully deployed app in action!  
https://www.youtube.com/watch?v=WtmCX8XYxM0&feature=youtu.be

Please look at my keynote presentation in the main folder of the repository for some more details! 
