using Newtonsoft.Json;


namespace LiveTracking.Models;
public class VehicleData
{
    [JsonProperty("GPSDeviceId")]
    public string GPSDeviceId { get; set; }

    [JsonProperty("IgnitionStatus")]
    public bool IgnitionStatus { get; set; }

    [JsonProperty("IsMoving")]
    public int IsMoving { get; set; }

    [JsonProperty("Position")]
    public Position Position { get; set; }

    [JsonProperty("DeviceTS")]
    public long DeviceTS { get; set; }

    [JsonProperty("VehiculeMotorise_Id")]
    public int VehiculeMotoriseId { get; set; }

    [JsonProperty("IgnitionOnDuration")]
    public int IgnitionOnDuration { get; set; }

    [JsonProperty("GsmSignalLevel")]
    public int GsmSignalLevel { get; set; }

    [JsonProperty("IsWashing")]
    public bool IsWashing { get; set; }
    [JsonProperty("IsSweeping")]
    public bool IsSweeping { get; set; }

    [JsonProperty("VehicleSpeed")]
    public int VehicleSpeed { get; set; }
    [JsonProperty("PositionSpeed")]
    public int PositionSpeed { get; set; }
    [JsonProperty("FuelLevel")]
    public int FuelLevel { get; set; }
    [JsonProperty("deviceId")]
    public string DeviceID { get; set; }

    [JsonProperty("VehicleMileage")]
    public double VehicleMileage { get; set; }

    [JsonProperty("FuelConsumed")]
    public int FuelConsumed { get; set; }


    [JsonProperty("DelegataireId")]
    public int DelegataireId { get; set; }

    [JsonProperty("VehiculeMotorise_Numero")]
    public string VehiculeMotoriseNumero { get; set; }

    [JsonProperty("Matricule")]
    public string Matricule { get; set; }

    [JsonProperty("VehiculeMotorise_Modele")]
    public string VehiculeMotoriseModele { get; set; }

    [JsonProperty("VehiculeMotorise_MarqueId")]
    public int VehiculeMotoriseMarqueId { get; set; }

    [JsonProperty("VehiculeMotorise_TypeMaterielId")]
    public int VehiculeMotoriseTypeMaterielId { get; set; }

    [JsonProperty("TypePrestationId")]
    public int TypePrestationId { get; set; }

    [JsonProperty("VehiculeMotorise_RFID")]
    public string VehiculeMotoriseRFID { get; set; }

    [JsonProperty("VehiculeMotorise_EtatId")]
    public int VehiculeMotoriseEtatId { get; set; }

    [JsonProperty("planningId")]
    public int? PlannigId { get; set; }

    [JsonProperty("CircuitId")]
    public int CircuitId { get; set; }


}

