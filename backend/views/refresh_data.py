from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from backend.data.Refresh_Exchange_Database import main as refresh_ix_data
from backend.data.Refresh_Facility_Database import main as refresh_fac_data
from backend.data.Refresh_Network_Database import main as refresh_net_data


@csrf_exempt
def refresh_all_data(request):
    try:
        # Call the refresh functions for IX, FAC, and NET
        refresh_ix_data()  # Refresh IX data and save to CSV
        refresh_fac_data()  # Refresh FAC data and save to CSV
        refresh_net_data()  # Refresh NET data and save to CSV

        return JsonResponse({"status": "All data refreshed successfully"}, status=200)
    
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
