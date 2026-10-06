-- The owner checks these values against the configured workspace/device.
-- A missing row or NULL binding is UNKNOWN, never an assumed zero audience.
SELECT current_workspace_id,current_device_id,current_installation_epoch_id,
 CASE WHEN current_workspace_id IS NOT NULL AND current_device_id IS NOT NULL
  AND current_installation_epoch_id IS NOT NULL THEN 1 ELSE 0 END AS binding_known
FROM collector_workspace_binding WHERE singleton=1;
