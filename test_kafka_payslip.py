import json
import base64
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:29092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Read a sample payslip file and encode it
with open('WinPayMar2026.pdf', 'rb') as f:
    file_base64 = base64.b64encode(f.read()).decode('utf-8')

message = {
    'tenant_id': 'tnt20261111001',
    'aisubscription_id': 'SUB2026111100066',
    'remarks': 'Processed by AI assistant',
    'filename': 'WinPayMar2026.pdf',
    'file_base64': file_base64,
    'extracted_data_json': json.dumps({
        'structured_data': {
            'full_name': 'Winnetou',
            'position': 'IT Freelance',
            'pay_period': 'MAR 2026',
            'take_home_pay': '7000000'
        },
        'raw_text': '... OCR text ...',
        'extraction_result': 'good'
    })
}

producer.send('payslip_extracted', value=message)
producer.flush()
print("Message sent")