export interface Paginated<T> {
  items: T[];
  limit: number;
  offset: number;
  returned_count: number;
}

export interface TokenPair {
  access_token: string;
  refresh_token: string;
  expires_at: string;
  token_type: string;
}

export interface Me {
  user_id: string;
  organization_id: string;
  email: string;
  roles: string[];
  permissions: string[];
}

export interface Factory {
  id: string;
  name: string;
  city: string;
  country: string;
}

export interface ProductionLine {
  id: string;
  name: string;
}

export interface Machine {
  id: string;
  name: string;
  machine_type: string;
  status: string;
}

export interface Device {
  id: string;
  serial_number: string;
  status: string;
  firmware_version: string;
}

export interface Sensor {
  id: string;
  metric: string;
  unit: string;
  min_operating_value: number;
  max_operating_value: number;
}

export interface Alert {
  id: string;
  severity: string;
  status: string;
  title: string;
  description: string;
  machine_id?: string;
  device_id?: string;
  triggered_at: string;
  resolved_at?: string;
}

export interface Incident {
  id: string;
  alert_id?: string;
  factory_id?: string;
  machine_id?: string;
  device_id?: string;
  sensor_id?: string;
  severity: string;
  status: string;
  title: string;
  description: string;
  assigned_to?: string;
  resolution_notes?: string;
  opened_at: string;
  resolved_at?: string;
  closed_at?: string;
}

export interface IncidentEvent {
  event_type: string;
  old_value?: string;
  new_value?: string;
  note?: string;
  created_at: string;
}

export interface TelemetryPoint {
  time: string;
  value: number;
}

export interface HealthStatus {
  postgres: string;
  redis: string;
  kafka: string;
  mqtt: string;
  influxdb: string;
}

export interface AlertEventMessage {
  alert_id: string;
  event_type: string;
  organization_id: string;
  severity: string;
  status: string;
  title: string;
  description: string;
  machine_id?: string;
  device_id?: string;
  timestamp: string;
}

export interface ApiErrorBody {
  error: { code: string; message: string; request_id: string };
}
