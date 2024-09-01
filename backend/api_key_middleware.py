from django.http import JsonResponse

class ApiKeyMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        api_key = request.headers.get('Authorization')

        if api_key != 'Api-Key your_expected_api_key_here':
            return JsonResponse({'error': 'Unauthorized'}, status=401)

        response = self.get_response(request)
        return response
