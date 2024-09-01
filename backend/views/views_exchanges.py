from django.http import JsonResponse
import pandas as pd
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

CSV_DIR = '/Databases'

@require_http_methods(["GET"])
@csrf_exempt
def exchange_data_view(request):
    """Load and filter exchange data from the CSV."""
    csv_file_path = ('C:/Users/jfent/Chadwork/Databases/exchange_data.csv')

    try:
        # Load the exchange data from CSV
        df = pd.read_csv(csv_file_path)
    except FileNotFoundError:
        return JsonResponse({"error": "Data not available. Please refresh the database."}, status=500)
    except pd.errors.EmptyDataError:
        return JsonResponse({"error": "The CSV file is empty."}, status=500)
    except Exception as e:
        return JsonResponse({"error": f"An error occurred: {str(e)}"}, status=500)

    # Get the filter parameters from the request
    country = request.GET.get('country')  # Filter by country
    city = request.GET.get('city')        # Filter by city

    # Apply filters based on country and city
    try:
        if country:
            df = df[df['country'] == country]
    except KeyError:
        return JsonResponse({"error": "Country column not found in the CSV file"}, status=500)

    try:
        if city:
            df = df[df['city'] == city]
    except KeyError:
        return JsonResponse({"error": "City column not found in the CSV file"}, status=500)

    # Sort the data by ix_count (descending order)
    sorted_df = df.sort_values(by='fac_count', ascending=False)

    # Select relevant columns
    columns = ['name', 'city', 'country', 'fac_count', 'net_count']
    result_df = sorted_df[columns]

    # Convert the top 20 rows to a dictionary
    top_20 = result_df.head(20).to_dict(orient='records')

    # Return the response with the Access-Control-Allow-Origin header
    response = JsonResponse(top_20, safe=False)
    response["Access-Control-Allow-Origin"] = "http://localhost:3000"  # Allow requests from your React app
    return response


