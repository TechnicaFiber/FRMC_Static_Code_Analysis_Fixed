namespace FireRakshak.FRMC.Core.Enums
{
    /// <summary>
    /// Defines the different roles that a user can have within the FireRakshak FRMC system.
    /// These roles are used for authorization, determining what actions a user is permitted to perform.
    /// </summary>
    public enum UserRole
    {
        /// <summary>
        /// Represents a standard user with limited access, typically for monitoring and basic operations.
        /// </summary>
        User = 0,
        /// <summary>
        /// Represents an administrator with full access to standard system functionalities, including channel/zone configuration and user management.
        /// </summary>
        Admin = 1,
        /// <summary>
        /// Represents the manufacturer with super-admin access, including factory hardware calibration, ATP, temperature correction, and user management.
        /// </summary>
        Manufacturer = 2
    }
}
